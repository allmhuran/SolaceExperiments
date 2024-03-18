using Microsoft.VisualBasic;
using SolaceSystems.Solclient.Messaging;
using System.Diagnostics;

namespace allmhuran.ConcurrentWithoutRetry
{
   public class Publication
   {
      /// <summary>
      /// build a connected publisher with a populated message ringbuffer
      /// </summary>
      public Publication(string vpn, string host, string userName, string password)
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
            ADPublishWindowSize = 1
         };
         _session = context.CreateSession(sprops, null, OnSessionEvent);
         _session.Connect();
      }

      public async Task Send(int count)
      {
         var topic = ContextFactory.Instance.CreateTopic("ping");
         IMessage[] messages = new IMessage[count];
         PingResult[] pings = new PingResult[count];
         for (int i = 0; i < count; i++)
         {
            var msg = ContextFactory.Instance.CreateMessage();
            msg.Destination = topic;
            msg.DeliveryMode = MessageDeliveryMode.Persistent;
            msg.TimeToLive = 1000;
            pings[i] = new();
            msg.CorrelationKey = pings[i];
            messages[i] = msg;
         }

         var sw = new Stopwatch();
         sw.Start();
         for (int i = 0; i < pings.Length; i++)
         {
            pings[i].ms = sw.ElapsedMilliseconds;
            _session.Send(messages[i]);
            pings[i].succeeded = await pings[i].tcs.Task;
            pings[i].ms = sw.ElapsedMilliseconds - pings[i].ms;
            Console.WriteLine($"{pings[i].ms,4}, {(pings[i].succeeded ? "ack" : "nack")}");
         }
         sw.Stop();
         sw.Reset();
         var avgMs = pings.Average(x => x.ms);
         var avgSuccess = pings.Average(p => p.succeeded ? 1.0 : 0.0);
         Console.WriteLine($"Sent {pings.Length}, average = {avgMs}, loss = {1 - avgSuccess:P1}");
      }

      private void OnSessionEvent(object? sender, SessionEventArgs e)
      {
         if (e.Event == SessionEvent.Acknowledgement || e.Event == SessionEvent.RejectedMessageError)
         {
            ((PingResult)e.CorrelationKey).tcs.SetResult(e.Event == SessionEvent.Acknowledgement ? true : false);
         }
      }

      private ISession _session;
      private record PingResult { public long ms; public bool succeeded = false; public TaskCompletionSource<bool> tcs = new(); }
   }
}
