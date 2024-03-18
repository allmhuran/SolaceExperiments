namespace allmhuran.PingPublisher
{
   internal class Program
   {
      private static async Task Main(string[] args)
      {
         var vpn = args.Length > 0 ? args[0] : Environment.GetEnvironmentVariable("solace.dev.vpn", EnvironmentVariableTarget.User);
         var host = args.Length > 1 ? args[1] : Environment.GetEnvironmentVariable("solace.dev.host", EnvironmentVariableTarget.User);
         var userName = args.Length > 2 ? args[2] : Environment.GetEnvironmentVariable("solace.dev.username", EnvironmentVariableTarget.User);
         var password = args.Length > 3 ? args[3] : Environment.GetEnvironmentVariable("solace.dev.password", EnvironmentVariableTarget.User);

         var pub = new Publication(vpn, host, userName, password);
         await pub.Ping(count: 1000, concurrentCount: 50, publishWindowSize: 50);
      }
   }
}
