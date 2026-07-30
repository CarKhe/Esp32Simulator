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
mqttClient.ApplicationMessageReceivedAsync += async e =>
{
    var topic = e.ApplicationMessage.Topic; // rancho/reles/riego/12/cmd
    var payload = e.ApplicationMessage.ConvertPayloadToString(); // on
    Console.WriteLine($"[Comando recibido] Topic: {topic} -> {payload}");

    var partes = topic.Split('/'); // ["rancho","reles","riego","12","cmd"]
    var tipo = partes[2];
    var id = partes[3];

    //Console.WriteLine($"[Simulando hardware] {tipo} {id} ahora esta: {payload}");

    var exito = ValidarCambio();

    var estadoReal = exito ? payload : "sin_cambio"; // si falló, no se movió del estado anterior

    var resultado = new { estado = estadoReal, exito };
    var json = System.Text.Json.JsonSerializer.Serialize(resultado);

    var estadoTopic = $"rancho/reles/{tipo}/{id}/estado";
    var mensajeEstado = new MqttApplicationMessageBuilder()
        .WithTopic(estadoTopic)
        .WithPayload(json)
        .WithRetainFlag()
        .Build();

    await mqttClient.PublishAsync(mensajeEstado);
    Console.WriteLine($"[Publicado] {estadoTopic} -> {json}");
};
//Abre la conexión real con el broker.
await mqttClient.ConnectAsync(options, CancellationToken.None);
Console.WriteLine("Conectado al broker MQTT");

//Le dice al broker "avísame cuando algo se publique en rancho/reles/cmd".
var subscribeOptions = mqttFactory.CreateSubscribeOptionsBuilder()
    .WithTopicFilter(f => f.WithTopic("rancho/reles/+/+/cmd"))
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


bool ValidarCambio()
{
    // Simula que a veces falla, para poder probar el camino de error
    return new Random().Next(100) > 10; // 90% exito, 10% falla simulada
}