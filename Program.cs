using MQTTnet;

//Creacion del factory para el cliente
var mqttFactory = new MqttClientFactory(); 
//crea el cliente
using var mqttClient = mqttFactory.CreateMqttClient();

//define a dónde conectarse: tu Mosquitto corriendo en Docker, puerto 1883.
var options = new MqttClientOptionsBuilder()
    .WithTcpServer("localhost", 1883)
    .Build();

//Evento de escucha al recibir mensajes
mqttClient.ApplicationMessageReceivedAsync += e =>
{
    var mensaje = e.ApplicationMessage.ConvertPayloadToString();
    Console.WriteLine($"[Comando recibido] Topic: {e.ApplicationMessage.Topic} -> {mensaje}");
    return Task.CompletedTask;
};

//Abre la conexión real con el broker.
await mqttClient.ConnectAsync(options, CancellationToken.None);
Console.WriteLine("Conectado al broker MQTT");

//Le dice al broker "avísame cuando algo se publique en rancho/reles/cmd".
var subscribeOptions = mqttFactory.CreateSubscribeOptionsBuilder()
    .WithTopicFilter(f => f.WithTopic("rancho/reles/cmd"))
    .Build();

//quiero recibir todo lo que se publique en tal topic
await mqttClient.SubscribeAsync(subscribeOptions, CancellationToken.None);
Console.WriteLine("Suscrito a rancho/reles/cmd, esperando comandos...");

while (true)
{
    //Envio de del Mensaje al Mqtt
    var temp = new Random().Next(15, 35);
    var message = new MqttApplicationMessageBuilder()
        .WithTopic("rancho/temp")
        .WithPayload(temp.ToString())
        .Build();

    //publicacion en el MQTT
    await mqttClient.PublishAsync(message, CancellationToken.None);
    Console.WriteLine($"[Publicado] rancho/temp -> {temp}");

    await Task.Delay(10000);
}