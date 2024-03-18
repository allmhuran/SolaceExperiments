using SolaceSystems.Solclient.Messaging;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Threading.Channels;

namespace allmhuran.GuaranteedOrderTransactionalPublisher
{
   public static class Extensions
   {
      public static double StdDev(this IEnumerable<long> values)
      {
         double ret = 0;
         int count = values.Count();
         if (count > 1)
         {
            double avg = values.Average();
            double sum = values.Sum(d => (d - avg) * (d - avg));
            ret = Math.Sqrt(sum / count);
         }
         return ret;
      }
   }

   public class Publication
   {
      /// <summary>
      /// build a connected publisher with a populated message ringbuffer
      /// </summary>
      public Publication(string vpn, string host, string userName, string password, string topicName)
      {
         var cfp = new ContextFactoryProperties() { SolClientLogLevel = SolLogLevel.Warning };
         cfp.LogToConsoleError();
         ContextFactory.Instance.Init(cfp);
         var context = ContextFactory.Instance.CreateContext(new(), null);

         var sprops = new SessionProperties
         {
            VPNName = vpn,
            Host = host,
            UserName = userName,
            Password = password,
            SSLValidateCertificate = false, // required due to our broker config
            ADPublishWindowSize = COMMIT_SIZE
         };

         var session = context.CreateSession(sprops, null, (s, e) => Console.WriteLine($"{e.Event} {e.ResponseCode}"));
         session.Connect();

         _transactedSession = session.CreateTransactedSession(new());

         _ringFreeBuffer = Channel.CreateBounded<IMessage>
         (
            new BoundedChannelOptions(COMMIT_SIZE + 1)
            {
               FullMode = BoundedChannelFullMode.Wait,
               SingleReader = true,
               SingleWriter = true
            }
         );

         var topic = ContextFactory.Instance.CreateTopic(topicName);

         for (int i = 0; i < COMMIT_SIZE + 1; i++)
         {
            var message = ContextFactory.Instance.CreateMessage();
            message.Destination = topic;
            message.DeliveryMode = MessageDeliveryMode.Persistent;
            message.TimeToLive = (long)TimeSpan.FromMinutes(10).TotalMilliseconds;
            message.BinaryAttachment = new byte[4];
            _ringFreeBuffer.Writer.TryWrite(message);
         }

         _ringFullBuffer = Channel.CreateBounded<IMessage>
         (
            new BoundedChannelOptions(COMMIT_SIZE + 1)
            {
               FullMode = BoundedChannelFullMode.Wait,
               SingleReader = true,
               SingleWriter = true
            }
         );

         var lvq = ContextFactory.Instance.CreateQueue(topic.Name + "/LVQ");

         session.Provision
         (
            lvq,
            new EndpointProperties
            {
               AccessType = EndpointProperties.EndpointAccessType.Exclusive,
               Permission = EndpointProperties.EndpointPermission.Consume,
               Quota = 0,
               RespectsMsgTTL = false
            },
            ProvisionFlag.WaitForConfirm | ProvisionFlag.IgnoreErrorIfEndpointAlreadyExists,
            null
         );

         _lvqMessage = ContextFactory.Instance.CreateMessage();
         _lvqMessage.Destination = lvq;
         _lvqMessage.DeliveryMode = MessageDeliveryMode.Persistent;
         _lvq = session.CreateBrowser(lvq, new BrowserProperties());

         _publishing = Task.Run(PublishAsync);
      }

      /// <summary>
      /// indicate that we will not publish any further messages, and wait for all currently
      /// enqueued messages to be comitted
      /// </summary>
      public void Complete()
      {
         // completing this writer will cause PublishAsync to exit once it finishes publishing
         // anything that has been enqueued
         _ringFullBuffer.Writer.Complete();
         _publishing.GetAwaiter().GetResult();
      }

      public async Task Enqueue(int payload)
      {
         var msg = await _ringFreeBuffer.Reader.ReadAsync();
         msg.SequenceNumber = payload;
         msg.BinaryAttachment = BitConverter.GetBytes(payload);
         if (!_ringFullBuffer.Writer.TryWrite(msg)) throw new Exception("impossible");
      }

      private async Task PublishAsync()
      {
         var sw = new Stopwatch();
         var results = new List<long>();
         int totalCount = 0;
         IMessage?[] commitBatch = new IMessage[COMMIT_SIZE];
         sw.Start();
         // this is the read head of the circular message buffer, containing enqueued
         // messages. This task will become true when a message becomes available, or false when the
         // channel writer is completed
         while (await _ringFullBuffer.Reader.WaitToReadAsync())
         {
            int retryDelayMs = 10, i = 0, j;
            bool retry = false;

            // grab enqueued messages until we fill the batch or there are no more immediately available
            while (i < commitBatch.Length && _ringFullBuffer.Reader.TryRead(out commitBatch[i])) i++;

            do
            {
               // send every message in the batch, and send the sequence number of the last message
               // in the batch to the LVQ
               for (j = 0; j < i; j++) _transactedSession.Send(commitBatch[j]);

               _lvqMessage.SequenceNumber = commitBatch[--j]!.SequenceNumber;
               _transactedSession.Send(_lvqMessage);

               try
               {
                  long ms = sw.ElapsedMilliseconds;
                  _transactedSession.Commit();
                  results.Add(sw.ElapsedMilliseconds - ms);
               }
               catch (OperationErrorException x)
               {
                  if (x.ReturnCode == ReturnCode.SOLCLIENT_ROLLBACK)
                  {
                     // if there's no storage available then wait and retry the batch
                     if (ContextFactory.Instance.GetLastSDKErrorInfo().SubCode == SDKErrorSubcode.SpoolOverQuota) retry = true;
                     // don't bother handling any other subcode here, treat them as fatal
                     else throw;
                  }
                  else if (x.ReturnCode == ReturnCode.SOLCLIENT_FAIL)
                  {
                     // if the transaction was rolled back then the message in the last value queue
                     // will not have the same sequence number as the last message in the commti
                     // batch, so wait and retry the batch otherwise the commit actually succeeded
                     retry = _lvq.GetNext().SequenceNumber != commitBatch[j]!.SequenceNumber;
                  }
                  else throw; // something weird happened
               }
               if (retry) await Task.Delay(retryDelayMs *= 2);
            } while (retry);

            // batch committed, put messages back into free list
            for (j = 0; j < i; j++) _ringFreeBuffer.Writer.TryWrite(commitBatch[j]!);

            totalCount += i;
         }
         sw.Stop();
         var seconds = sw.Elapsed.TotalSeconds;
         Console.WriteLine($"published {totalCount} messages in {seconds:F2} seconds at {totalCount / seconds:F2} msgs/sec");
         Console.WriteLine($"avg commit ms   = {results.Average(),6:F2}");
         Console.WriteLine($"min commit ms   = {results.Min(),6:F2}");
         Console.WriteLine($"max commit ms   = {results.Max(),6:F2}");
         Console.WriteLine($"stdev commit ms = {results.StdDev(),6:F2}");
      }

      private const int COMMIT_SIZE = 200;
      private readonly IBrowser _lvq;
      private readonly IMessage _lvqMessage;
      private readonly Task _publishing;
      private readonly Channel<IMessage> _ringFreeBuffer;
      private readonly Channel<IMessage> _ringFullBuffer;
      private readonly ITransactedSession _transactedSession;
   }
}