using SolaceSystems.Solclient.Messaging;
using System.Buffers.Binary;
using System.Threading.Channels;

namespace allmhuran.GuaranteedOrderTransactionalPublisher
{
   public class Subscription
   {
      public Subscription(string vpn, string host, string userName, string password, string topicName)
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
            SSLValidateCertificate = false
         };
         var session = context.CreateSession(sprops, null, (s, e) => Console.WriteLine($"{e.Event} {e.ResponseCode}"));
         session.Connect();
         IQueue queue = ContextFactory.Instance.CreateQueue(session.Properties.UserName + "/subscriptions/" + topicName);
         session.Provision
         (
            queue,
            new EndpointProperties
            {
               Permission = EndpointProperties.EndpointPermission.Consume,
               AccessType = EndpointProperties.EndpointAccessType.Exclusive,
               MaxMsgRedelivery = 1,
               RespectsMsgTTL = true
            },
            ProvisionFlag.IgnoreErrorIfEndpointAlreadyExists | ProvisionFlag.WaitForConfirm,
            null
         );

         ITopic topic = ContextFactory.Instance.CreateTopic(topicName);
         session.Subscribe(queue, topic, SubscribeFlag.WaitForConfirm, null);

         _flow = session.CreateFlow
         (
            flowProperties: new FlowProperties()
            {
               AckMode = MessageAckMode.ClientAck,
               FlowStartState = false,
               WindowSize = 100,
               MaxUnackedMessages = 1000
            },
            endPoint: queue,
            subscription: null,
            messageEventHandler: OnMessageReceived,
            flowEventHandler: (_, _) => { }
         );

         _buffer = Channel.CreateUnbounded<int>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });
      }

      public IAsyncEnumerable<int> ReadAllAsync(CancellationToken ct = default)
      {
         _flow.Start();
         return _buffer.Reader.ReadAllAsync(ct);
      }

      private void OnMessageReceived(object? sender, MessageEventArgs args)
      {
         int payload = BitConverter.ToInt32(args.Message.BinaryAttachment, 0);
         _flow.Ack(args.Message.ADMessageId);
         _buffer.Writer.TryWrite(payload);
      }

      private readonly Channel<int> _buffer;
      private readonly IFlow _flow;
   }
}
