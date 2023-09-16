/*
 * 
Informatics Systems in IoT Course:
"POC of IoT system for monitoring drug stores in pharmacies"
@ Roberto Hernandez Ruiz
*
*/

#include <WiFi.h>
#include <Wire.h>
#include <Servo.h>
#include <ArduinoJson.h>
#include <HTTPClient.h>
#include <Adafruit_BME280.h>
#include <Adafruit_Sensor.h>
#include <Adafruit_NeoPixel.h>

#define SEALEVELPRESSURE_HPA (1013.25)
#define ARDUINO_RUNNING_CORE 0
#define ARDUINO_RUNNING_CORE 1

Adafruit_BME280 bme; 

const char* ssid = "my-wifi";
const char* password =  "...";
const char* ip = "my-ip"
 
Servo servo;
static const int servomotor = 26; 
static const int PIR = 27;

int numPresence;

#define LED            2 
#define NUMPIXELS      12 //number of leds
Adafruit_NeoPixel pixels = Adafruit_NeoPixel(NUMPIXELS, LED, NEO_GRB + NEO_KHZ800);
int delayval = 100;

WiFiClient wifi;
WiFiServer server(80); 

TaskHandle_t Task1;
TaskHandle_t Task2;

void initWifi(){
  WiFi.begin(ssid, password);
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.println("Connecting...");
  } 
  Serial.println("Connected. IP address set:");
  Serial.println(WiFi.localIP());
}

void initBME(){
  if (!bme.begin(0x76)) {
    Serial.println("Could not find a valid BME280 sensor, check wiring!");
    while (1);
  }
}

void setup(){
  Serial.begin(115200);
  initWifi();
  initBME();
  
  servo.attach(servomotor);
  numPresence=0;
  servo.write(0);
  pinMode(PIR, INPUT_PULLUP);
  pinMode(servomotor, OUTPUT);

  pixels.begin();
  pixels.setBrightness(150);

  server.begin();

  xTaskCreatePinnedToCore(loop1, "loop1", 4096, NULL, 1, NULL, ARDUINO_RUNNING_CORE);
  xTaskCreatePinnedToCore(loop2, "loop2", 4096, NULL, 1, NULL, ARDUINO_RUNNING_CORE);

}


void loop1( void * pvParameters ){
  
  for(;;){
    HTTPClient http;
    
    char json[] = "{\"sensor\":\"gps\",\"time\":1351824120,\"data\":[48.756080,2.302038]}";
    StaticJsonDocument<200> doc;
    
    // Sensors measurements and data retrieval
    doc["temperature"] = bme.readTemperature(); 
    doc["pressure"] = bme.readPressure() / 100.0F;
    doc["humidity"] = bme.readHumidity();

    int sensorval = digitalRead(PIR);
    if (sensorval == HIGH) {
      doc["presence"] = "True";
    }else{
      doc["presence"] = "False";
    }
    
    String json_string;
    serializeJson(doc, json_string);
    Serial.print(json_string); 
    
    http.begin(ip + ":5000/sensor_values"); 
    http.addHeader("Content-Type", "application/json"); 

    // Send data to server
    int httpResponseCode = http.POST(json_string); 
    if (httpResponseCode > 0) {
      String response = http.getString(); 
      Serial.println(httpResponseCode);
      Serial.println(response);
    } else {
      Serial.print("Error on sending POST Request: "); 
      Serial.println(httpResponseCode); 
    }
    http.end();
    delay (3000);
  }
}



void loop2( void * pvParameters ){
  
  for(;;){
    if (WiFi.status() == WL_CONNECTED) {
        WiFiClient client = server.available();    
        if (client) {      
           String currentLine = "";                // make a String to hold incoming data from the client
           while (client.connected()) {            // loop while the client's connected
              if (client.available()) {             // if there's bytes to read from the client,
                char c = client.read();             // read a byte, then
                Serial.write(c);                    // print it out the serial monitor
                if (c == '\n') {                    // if the byte is a newline character
                  if (currentLine.length() == 0) {
                    // Encabezados HTTP empiezan con respuesta y tipo de contenido
                      client.println("HTTP/1.1 200 OK");
                      client.println("Content-type:text/html");
                      client.println();
                      break;
                  } else {    // if you got a newline, then clear currentLine:
                      currentLine = "";
                  }
                } else if (c != '\r') {  // if you got anything else but a carriage return character,
                  currentLine += c;      // add it to the end of the currentLine
                }

                // Presence detected: servo motor moves!
                if (currentLine.endsWith("GET /P")) {
                  servo.write(180); 
                  delay(800);
                  servo.write(0);
                }       
                // Temperature exceeds 25ºC: red LEDs!
                if (currentLine.endsWith("GET /T")) {        
                  for(int i=0;i<NUMPIXELS;i++){
                    pixels.setPixelColor(i, pixels.Color(255,0,0)); 
                    pixels.show(); 
                    delay(delayval); 
                  }
                  for(int i=0;i<NUMPIXELS;i++){
                    pixels.setPixelColor(i, pixels.Color(0,0,0)); 
                    pixels.show(); 
                  }
                }   
                // Temperature is lower than 20ºC: blue LEDs!
                if (currentLine.endsWith("GET /L")) {        
                  for(int i=0;i<NUMPIXELS;i++){
                    pixels.setPixelColor(i, pixels.Color(0,0,255)); 
                    pixels.show(); 
                    delay(delayval); 
                  }
                  for(int i=0;i<NUMPIXELS;i++){
                    pixels.setPixelColor(i, pixels.Color(0,0,0)); 
                    pixels.show(); 
                  }
                }  
                // Humidity exceeds 45%: green LEDs!
                if (currentLine.endsWith("GET /H")) {        
                  for(int i=0;i<NUMPIXELS;i++){
                    pixels.setPixelColor(i, pixels.Color(0,255,0)); 
                    pixels.show(); 
                    delay(delayval); 
                  }
                  for(int i=0;i<NUMPIXELS;i++){
                    pixels.setPixelColor(i, pixels.Color(0,0,0)); 
                    pixels.show(); 
                  }
                }       
              }
            }  
         client.stop();
         }
      } 
    delay (3000);
  }
 }


void loop() {
}
 
    
