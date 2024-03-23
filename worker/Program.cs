using System;
using System.Data.Common;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Newtonsoft.Json;
using Npgsql;
using StackExchange.Redis;

namespace Worker
{
    public class Program
    {
        public static int Main(string[] args)
        {
            try
            {
                var pguser = Environment.GetEnvironmentVariable("POSTGRES_USER");
                var pgpassword = Environment.GetEnvironmentVariable("POSTGRES_PASSWORD");
                var pgdbname = Environment.GetEnvironmentVariable("POSTGRES_DATABASE");
                var pghost = Environment.GetEnvironmentVariable("POSTGRES_HOST");
                var redis_host = Environment.GetEnvironmentVariable("REDIS_HOST");
                string connectionString = $"Host={pghost};Database={pgdbname};User Id={pguser};Password={pgpassword};";
                var pgsql = OpenDbConnection(connectionString);
                var redisConn = OpenRedisConnection(redis_host);
                var redis = redisConn.GetDatabase();

                // Keep alive is not implemented in Npgsql yet. This workaround was recommended:
                // https://github.com/npgsql/npgsql/issues/1214#issuecomment-235828359
                var keepAliveCommand = pgsql.CreateCommand();
                keepAliveCommand.CommandText = "SELECT 1";

                var definition = new { vote = "", voter_id = "" };
                while (true)
                {
                    // Slow down to prevent CPU spike, only query each 100ms
                    Thread.Sleep(100);

                    // Reconnect redis if down
                    if (redisConn == null || !redisConn.IsConnected) {
                        Console.WriteLine("Reconnecting Redis");
                        redisConn = OpenRedisConnection(redis_host);
                        redis = redisConn.GetDatabase();
                    }
                    string json = redis.ListLeftPopAsync("votes").Result;
                    if (json != null)
                    {
                        var vote = JsonConvert.DeserializeAnonymousType(json, definition);
                        Console.WriteLine($"Processing vote for '{vote.vote}' by '{vote.voter_id}'");
                        // Reconnect DB if down
                        if (!pgsql.State.Equals(System.Data.ConnectionState.Open))
                        {
                            Console.WriteLine("Reconnecting DB");
                            pgsql = OpenDbConnection($"Host={pghost};Username={pguser};Password={pgpassword};Database={pgdbname};");
                        }
                        else
                        { // Normal +1 vote requested
                            UpdateVote(pgsql, vote.voter_id, vote.vote);
                        }
                    }
                    else
                    {
                        keepAliveCommand.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.ToString());
                return 1;
            }
        }

        private static NpgsqlConnection OpenDbConnection(string connectionString)
        {
            NpgsqlConnection connection = null;

            while (true)
            {
                try
                {
                    connection = new NpgsqlConnection(connectionString);
                    connection.Open();
                    break;
                }
                catch (SocketException ex)
                {
                    Console.Error.WriteLine($"SocketException while trying to connect to DB: {ex.Message}");
                    Thread.Sleep(1000); // Wait before retrying
                }
                catch (Npgsql.NpgsqlException ex)
                {
                    // Catching PostgreSQL-specific exceptions for more detailed context
                    Console.Error.WriteLine($"NpgsqlException while trying to connect to DB: {ex.Message}");
                    if (ex.InnerException != null)
                    {
                        Console.Error.WriteLine($"Inner Exception: {ex.InnerException.Message}");
                    }
                    Thread.Sleep(1000); // Wait before retrying
                }
                catch (DbException ex)
                {
                    Console.Error.WriteLine($"DbException while trying to connect to DB: {ex.Message}");
                    Thread.Sleep(1000); // Wait before retrying
                }
                catch (Exception ex)
                {
                    // Catch-all for any other unexpected exceptions
                    Console.Error.WriteLine($"Unexpected exception while trying to connect to DB: {ex.Message}");
                    return null; // Or consider re-throwing the exception after logging
                }
            }

            if (connection != null)
            {
                try
                {
                    var command = connection.CreateCommand();
                    command.CommandText = @"CREATE TABLE IF NOT EXISTS votes (
                                                id VARCHAR(255) NOT NULL UNIQUE,
                                                vote VARCHAR(255) NOT NULL
                                            )";
                    command.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"Error while trying to create 'votes' table: {ex.Message}");
                    // Handle or log the error as needed
                }
            }

            return connection;
        }

        private static ConnectionMultiplexer OpenRedisConnection(string hostname)
        {
            // Use IP address to workaround https://github.com/StackExchange/StackExchange.Redis/issues/410
            var ipAddress = GetIp(hostname);
            Console.WriteLine($"Found redis at {ipAddress}");

            while (true)
            {
                try
                {
                    Console.Error.WriteLine("Connecting to redis");
                    return ConnectionMultiplexer.Connect(ipAddress);
                }
                catch (RedisConnectionException)
                {
                    Console.Error.WriteLine("Waiting for redis");
                    Thread.Sleep(1000);
                }
            }
        }

        private static string GetIp(string hostname)
            => Dns.GetHostEntryAsync(hostname)
                .Result
                .AddressList
                .First(a => a.AddressFamily == AddressFamily.InterNetwork)
                .ToString();

        private static void UpdateVote(NpgsqlConnection connection, string voterId, string vote)
        {
            var command = connection.CreateCommand();
            try
            {
                command.CommandText = "INSERT INTO votes (id, vote) VALUES (@id, @vote)";
                command.Parameters.AddWithValue("@id", voterId);
                command.Parameters.AddWithValue("@vote", vote);
                command.ExecuteNonQuery();
            }
            catch (DbException)
            {
                command.CommandText = "UPDATE votes SET vote = @vote WHERE id = @id";
                command.ExecuteNonQuery();
            }
            finally
            {
                command.Dispose();
            }
        }
    }
}