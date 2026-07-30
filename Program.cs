using MQTTnet;

//Creacion del factory para el cliente
var mqttFactory = new MqttClientFactory(); 
//crea el cliente
using var mqttClient = mqttFactory.CreateMqttClient();

var estadosActuales = new Dictionary<string, string>(); // NUEVO: clave "tipo/id", valor "on"/"off"

//define a dónde conectarse: tu Mosquitto corriendo en Docker, puerto 1883.
var options = new MqttClientOptionsBuilder()
    .WithTcpServer("localhost", 1883)
    .WithWillTopic("rancho/esp32/conexion")
    .WithWillPayload("offline")
    .WithWillRetain(true)
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
    var exito = ValidarCambio();

    var estadoReal = exito ? payload : "sin_cambio"; // si falló, no se movió del estado anterior

     estadosActuales[$"{tipo}/{id}"] = estadoReal; // NUEVO: se guarda en memoria

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

// NUEVO: avisa que esta realmente online, apenas se conecta
var mensajeOnline = new MqttApplicationMessageBuilder()
    .WithTopic("rancho/esp32/conexion")
    .WithPayload("online")
    .WithRetainFlag()
    .Build();

await mqttClient.PublishAsync(mensajeOnline);
Console.WriteLine("[Publicado] rancho/esp32/conexion -> online");

await mqttClient.SubscribeAsync(subscribeOptions, CancellationToken.None);
Console.WriteLine("Suscrito a rancho/reles/+/+/cmd, esperando comandos...");

int contador = 0; // NUEVO: cuenta ciclos para el heartbeat

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

    contador++;
    
    if (contador % 3 == 0) // NUEVO: cada 3 ciclos de 10s = cada 30s
    {
        foreach (var kv in estadosActuales)
        {
            var clavePartes = kv.Key.Split('/'); // tipo, id
            var estadoTopicHb = $"rancho/reles/{clavePartes[0]}/{clavePartes[1]}/estado";
            var resultadoHb = new { estado = kv.Value, exito = true };
            var jsonHb = System.Text.Json.JsonSerializer.Serialize(resultadoHb);

            var mensajeHb = new MqttApplicationMessageBuilder()
                .WithTopic(estadoTopicHb)
                .WithPayload(jsonHb)
                .WithRetainFlag()
                .Build();

            await mqttClient.PublishAsync(mensajeHb);
            Console.WriteLine($"[Heartbeat] {estadoTopicHb} -> {jsonHb}");
        }
    }

    await Task.Delay(10000);
}


bool ValidarCambio()
{
    // Simula que a veces falla, para poder probar el camino de error
    return new Random().Next(100) > 10; // 90% exito, 10% falla simulada
}