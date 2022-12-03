using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using Npgsql;
using Newtonsoft.Json;

class Receive
{
    //Npgsql
    private static string connstring = String.Format("Server={0};Port={1};" +
        "User Id={2};Password={3};Database={4};",
        "localhost", 5432, "postgres",
        "password", "RFID");
    private static NpgsqlConnection? conn;

    private static NpgsqlDataReader Logs(string sql)
    {
        conn = new NpgsqlConnection(connstring);
        conn.Open();

        string dml = sql;
        NpgsqlDataReader? reader = null;

        using (NpgsqlCommand cmd = new NpgsqlCommand(dml, conn))
        {
            try
            {
                reader = cmd.ExecuteReader();
            }
            catch (Exception e)
            {
                conn.Close();
                Console.WriteLine("Erro : " + e);
            }
        }
        return reader;
    }

    //Serialização
    public static IEnumerable<Dictionary<string, object>> Serialize(NpgsqlDataReader reader)
    {
        var results = new List<Dictionary<string, object>>();
        var cols = new List<string>();
        for (var i = 0; i < reader.FieldCount; i++)
            cols.Add(reader.GetName(i));

        while (reader.Read())
            results.Add(SerializeRow(cols, reader));

        return results;
    }

    private static Dictionary<string, object> SerializeRow(IEnumerable<string> cols,
                                                NpgsqlDataReader reader)
    {
        var result = new Dictionary<string, object>();
        foreach (var col in cols)
            result.Add(col, reader[col]);
        return result;
    }

    public static void Main()
    {
        NpgsqlDataReader? reader = null;

        var factory = new ConnectionFactory() { HostName = "localhost" };

        var connection = factory.CreateConnection();
        var channel = connection.CreateModel();

        channel.QueueDeclare(
            "RRtag",
            durable: false,
            exclusive: false,
            autoDelete: false,
            arguments: null);

        var consumer = new EventingBasicConsumer(channel);

        consumer.Received += (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);
            Console.WriteLine($"Received request: {message}");

            reader = Logs(message);
            var r = Serialize(reader);
            string json = JsonConvert.SerializeObject(r, Formatting.Indented);

            var replyMessage = json;
            var replyBody = Encoding.UTF8.GetBytes(replyMessage);

            channel.BasicPublish("", ea.BasicProperties.ReplyTo, null, replyBody);
        };

        channel.BasicConsume(
            queue: "RRtag", 
            autoAck: true, 
            consumer: consumer);

        Console.WriteLine("Microsserviço RRtag");
        Console.ReadKey();
    }
}