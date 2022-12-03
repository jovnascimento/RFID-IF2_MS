using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using Npgsql;

class Receive
{
    //Npgsql
    private static string connstring = String.Format("Server={0};Port={1};" +
        "User Id={2};Password={3};Database={4};",
        "localhost", 5432, "postgres",
        "password", "RFID");
    private static NpgsqlConnection? conn;

    private static void Tag(string sql)
    {
        conn = new NpgsqlConnection(connstring);
        conn.Open();

        string dml = sql;

        using (NpgsqlCommand cmd = new NpgsqlCommand(dml, conn))
        {
            try
            {
                cmd.ExecuteNonQuery();
                conn.Close();
            }
            catch (Exception e)
            {
                conn.Close();
                Console.WriteLine("Erro: " + e);
            }
        }
    }

    public static void Main()
    {
        var factory = new ConnectionFactory()
        {
            HostName = "localhost"
        };
        using (var connection = factory.CreateConnection())
        using (var channel = connection.CreateModel())
        {
            channel.QueueDeclare(queue: "Itag",
                                 durable: false,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);

            var consumer = new EventingBasicConsumer(channel);

            consumer.Received += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                Tag(message);
                Console.WriteLine(" [x] Received {0}", message);
            };
            channel.BasicConsume(queue: "Itag",
                                 autoAck: true,
                                 consumer: consumer);

            Console.WriteLine("Microserviço Itag");
            Console.ReadKey();
        }
    }
}