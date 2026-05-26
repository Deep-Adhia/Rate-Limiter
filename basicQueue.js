const redis = require("redis");

async function run() {
  // We need two separate connections because the consumer will block its connection while waiting
  const producerClient = redis.createClient();
  const consumerClient = redis.createClient();

  await producerClient.connect();
  await consumerClient.connect();

  const QUEUE_KEY = "jobs:invoice_processing";

  // Helper to format timestamps
  function formatTime(timestampMs) {
    const date = new Date(timestampMs);
    const hours = date.getHours().toString().padStart(2, '0');
    const minutes = date.getMinutes().toString().padStart(2, '0');
    const seconds = date.getSeconds().toString().padStart(2, '0');
    const milliseconds = date.getMilliseconds().toString().padStart(3, '0');
    return `${hours}:${minutes}:${seconds}.${milliseconds}`;
  }

  // --- 1. THE PRODUCER (Sends jobs to the queue) ---
  async function startProducer() {
    let jobId = 1;
    
    // Send a new job every 3 seconds
    const intervalId = setInterval(async () => {
      const jobData = {
        id: jobId++,
        task: "Generate Invoice PDF",
        createdAt: Date.now()
      };

      const now = Date.now();
      console.log(`[Time: ${formatTime(now)}] 📤 [PRODUCER] Sending Job #${jobData.id} to queue...`);

      // Push to the left (head) of the queue
      await producerClient.lPush(QUEUE_KEY, JSON.stringify(jobData));
    }, 3000);

    return intervalId;
  }

  // --- 2. THE CONSUMER (Pulls and processes jobs) ---
  async function startConsumer() {
    console.log(`[Time: ${formatTime(Date.now())}] ⚙️ [CONSUMER] Worker started. Waiting for jobs...`);

    while (true) {
      try {
        // BRPOP blocks and waits. The '0' timeout means "wait forever" until a job is available
        // It returns an array: [key_name, popped_value]
        const result = await consumerClient.brPop(QUEUE_KEY, 0);
        
        if (result) {
          const now = Date.now();
          const job = JSON.parse(result.element);
          
          console.log(`[Time: ${formatTime(now)}] 📥 [CONSUMER] Received Job #${job.id}!`);
          console.log(`   └─ Job details: ${job.task} | Created at: ${formatTime(job.createdAt)}`);
          
          // Simulate some heavy work (PDF generation taking 1.5 seconds)
          console.log(`   └─ Processing Job #${job.id}...`);
          await new Promise((r) => setTimeout(r, 1500));
          
          console.log(`   └─ ✅ Job #${job.id} Complete! Worker goes back to waiting...`);
        }
      } catch (err) {
        console.error("Consumer Error:", err);
        break;
      }
    }
  }

  // --- TEST SIMULATION ---
  // Start the consumer in the background (uses its own connection)
  startConsumer();

  // Start the producer after 2 seconds
  await new Promise((r) => setTimeout(r, 2000));
  const producerInterval = await startProducer();

  // Let it run for 15 seconds to watch the exchange
  await new Promise((r) => setTimeout(r, 15000));

  // Cleanup
  console.log("\n⏳ Cleaning up and shutting down...");
  clearInterval(producerInterval);
  await producerClient.del(QUEUE_KEY);
  await producerClient.quit();
  await consumerClient.quit();
}

run().catch(console.error);
