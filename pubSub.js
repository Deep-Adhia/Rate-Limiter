const redis = require("redis");

async function run() {
  // We need 3 client connections: 2 for the subscribers (which must listen continuously) and 1 for the publisher
  const emailSubscriber = redis.createClient();
  const smsSubscriber = redis.createClient();
  const publisher = redis.createClient();

  await emailSubscriber.connect();
  await smsSubscriber.connect();
  await publisher.connect();

  const CHANNEL = "user:registered";

  // Helper to format timestamps
  function formatTime(timestampMs) {
    const date = new Date(timestampMs);
    const hours = date.getHours().toString().padStart(2, '0');
    const minutes = date.getMinutes().toString().padStart(2, '0');
    const seconds = date.getSeconds().toString().padStart(2, '0');
    const milliseconds = date.getMilliseconds().toString().padStart(3, '0');
    return `${hours}:${minutes}:${seconds}.${milliseconds}`;
  }

  // --- SUBSCRIBER 1: Email Notification Service ---
  async function startEmailService() {
    console.log(`[Time: ${formatTime(Date.now())}] 📧 [EMAIL SERVICE] Subscribing to channel: "${CHANNEL}"...`);
    await emailSubscriber.subscribe(CHANNEL, (message) => {
      const data = JSON.parse(message);
      console.log(`[Time: ${formatTime(Date.now())}] 📧 [EMAIL SERVICE] Received event! Sending welcome email to ${data.email}...`);
    });
  }

  // --- SUBSCRIBER 2: SMS Notification Service ---
  async function startSMSService() {
    console.log(`[Time: ${formatTime(Date.now())}] 📱 [SMS SERVICE] Subscribing to channel: "${CHANNEL}"...`);
    await smsSubscriber.subscribe(CHANNEL, (message) => {
      const data = JSON.parse(message);
      console.log(`[Time: ${formatTime(Date.now())}] 📱 [SMS SERVICE] Received event! Sending welcome SMS to ${data.phone}...`);
    });
  }

  // --- PUBLISHER: User Registration Trigger ---
  async function registerUser(username, email, phone) {
    const userData = { username, email, phone, timestamp: Date.now() };
    const now = Date.now();
    
    console.log(`\n[Time: ${formatTime(now)}] 🚀 [WEBSITE] User "${username}" registered! Publishing event to channel...`);
    
    // Publish message. It returns the number of active subscribers who received it
    const subscriberCount = await publisher.publish(CHANNEL, JSON.stringify(userData));
    console.log(`   └─ Event delivered to ${subscriberCount} active subscribers.`);
  }

  // --- TEST SIMULATION ---
  // Start both subscribers listening in the background
  await startEmailService();
  await startSMSService();

  // Wait 2 seconds, then publish the first user registration
  await new Promise((r) => setTimeout(r, 2000));
  await registerUser("alice_99", "alice@example.com", "+1-555-0199");

  // Wait 3 seconds, then publish another user registration
  await new Promise((r) => setTimeout(r, 3000));
  await registerUser("bob_secure", "bob@example.com", "+1-555-0144");

  // Wait 2 seconds and clean up
  await new Promise((r) => setTimeout(r, 2000));
  console.log("\n⏳ Shutting down pub/sub services...");
  
  await emailSubscriber.unsubscribe(CHANNEL);
  await smsSubscriber.unsubscribe(CHANNEL);
  
  await emailSubscriber.quit();
  await smsSubscriber.quit();
  await publisher.quit();
}

run().catch(console.error);
