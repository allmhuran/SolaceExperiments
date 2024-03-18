using Microsoft.VisualBasic;
using SolaceSystems.Solclient.Messaging;
using System.Diagnostics;
using System.Threading.Channels;

namespace allmhuran.PingPublisher
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
      public Publication(string vpn, string host, string userName, string password)
      {
         _vpn = vpn;
         _host = host;
         _userName = userName;
         _password = password;
      }

      public async Task Ping(int count, int maxUnacked = 1, int publishWindowSize = 1)
      {
         // connect
         var cfp = new ContextFactoryProperties() { SolClientLogLevel = SolLogLevel.Warning };
         cfp.LogToConsoleError();
         ContextFactory.Instance.Init(cfp);
         var context = ContextFactory.Instance.CreateContext(new(), null);

         var sprops = new SessionProperties
         {
            VPNName = _vpn,
            Host = _host,
            UserName = _userName,
            Password = _password,
            SSLValidateCertificate = false, // required due to our broker config
            ADPublishWindowSize = publishWindowSize
         };
         var topic = ContextFactory.Instance.CreateTopic("ping");
         using var session = context.CreateSession(sprops, null, OnSessionEvent);
         session.Connect();

         // preallocate
         IMessage[] messages = new IMessage[count];
         PingResult[] pings = new PingResult[count];

         for (int i = 0; i < count; i++)
         {
            var msg = ContextFactory.Instance.CreateMessage();
            msg.Destination = topic;
            msg.DeliveryMode = MessageDeliveryMode.Persistent;
            msg.TimeToLive = 5000;
            messages[i] = msg;
            pings[i] = new PingResult { id = i + 1 };
         }

         _sem = new SemaphoreSlim(maxUnacked, maxUnacked);
         _output = Channel.CreateUnbounded<int>();
         _ = Task.Run(async () => { await foreach (var i in _output.Reader.ReadAllAsync()) if (i % 10 == 0) Console.WriteLine(i); });

         _sw.Reset();
         _sw.Start();

         // send
         for (int i = 0; i < count; i++)
         {
            pings[i].ms = _sw.ElapsedMilliseconds;
            messages[i].CorrelationKey = pings[i];
            _sem.Wait();
            if (session.Send(messages[i]) != ReturnCode.SOLCLIENT_OK) throw new Exception("send failure");
         }

         _sw.Stop();
         _output.Writer.Complete();
         await _output.Reader.Completion;

         var avgMs = pings.Average(x => x.ms);
         var stdev = pings.Select(x => x.ms).StdDev();
         var loss = pings.Average(p => p.tcs.Task.Result ? 0.0 : 1.0);
         Console.WriteLine($"Count = {pings.Length} avg = {avgMs}ms stdev = {stdev:F2}ms loss = {loss:P1} rate = {1000.0 * pings.Length / _sw.ElapsedMilliseconds:F3}msgs/sec");
      }

      private void OnSessionEvent(object? sender, SessionEventArgs e)
      {
         if (e.Event == SessionEvent.Acknowledgement || e.Event == SessionEvent.RejectedMessageError)
         {
            var result = e.CorrelationKey as PingResult ?? throw new Exception("wha?");
            result.ms = _sw.ElapsedMilliseconds - result.ms;
            result.tcs.SetResult(e.Event == SessionEvent.Acknowledgement);
            _sem.Release();
            _output.Writer.TryWrite(result.id);
         }
      }

      private readonly string _host;
      private readonly string _password;
      private readonly string _userName;
      private readonly string _vpn;
      private Channel<int> _output;
      private SemaphoreSlim _sem;
      private Stopwatch _sw = new();
      private record PingResult { public int id; public long ms; public TaskCompletionSource<bool> tcs = new(); }
   }
}
