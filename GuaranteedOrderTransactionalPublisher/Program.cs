using System.Diagnostics;

namespace allmhuran.GuaranteedOrderTransactionalPublisher
{
   internal class Program
   {
      private static async Task Main(string[] args)
      {
         var vpn = args.Length > 0 ? args[0] : Environment.GetEnvironmentVariable("solace.dev.vpn", EnvironmentVariableTarget.User);
         var host = args.Length > 1 ? args[1] : Environment.GetEnvironmentVariable("solace.dev.host", EnvironmentVariableTarget.User);
         var userName = args.Length > 2 ? args[2] : Environment.GetEnvironmentVariable("solace.dev.username", EnvironmentVariableTarget.User);
         var password = args.Length > 3 ? args[3] : Environment.GetEnvironmentVariable("solace.dev.password", EnvironmentVariableTarget.User);

         var sub = new Subscription(vpn, host, userName, password, "topic1");
         var pub = new Publication(vpn, host, userName, password, "topic1");

         int countToPublish = 10000;
         Console.WriteLine($"publishing {countToPublish}");
         for (int i = 1; i <= countToPublish; i++) await pub.Enqueue(i);
         pub.Complete();
         Console.WriteLine("draining...");
         CancellationTokenSource cts = new(TimeSpan.FromSeconds(8));
         int last = 0;
         try
         {
            await foreach (int payload in sub.ReadAllAsync(cts.Token)) last = payload;
         }
         catch (OperationCanceledException) { }

         Console.WriteLine($"last payload received was {last}");
         Console.WriteLine("any key to exit");
         Console.ReadKey();
      }
   }
}