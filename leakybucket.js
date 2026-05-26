const redis = require("redis");

async function run() {
  const client = redis.createClient();
  await client.connect();

  const BUCKET_KEY = "ratelimit:leakybucket:global_queue";
  const BUCKET_CAPACITY = 5; // Max requests allowed to wait in queue
  const LEAK_RATE_MS = 10000; // Processes 1 request every 10 seconds (Slower for GUI inspection)

  // Helper function to format timestamp into human-readable clock time
  function formatTime(timestampMs) {
    const date = new Date(Number(timestampMs));
    const hours = date.getHours().toString().padStart(2, '0');
    const minutes = date.getMinutes().toString().padStart(2, '0');
    const seconds = date.getSeconds().toString().padStart(2, '0');
    const milliseconds = date.getMilliseconds().toString().padStart(3, '0');
    return `${hours}:${minutes}:${seconds}.${milliseconds}`;
  }

  // --- 1. THE API ENDPOINT SIMULATION (Adding to the Bucket) ---
  async function handleIncomingRequest(requestId) {
    // Check current size of the bucket
    const currentQueueLength = await client.lLen(BUCKET_KEY);
    const now = Date.now();

    if (currentQueueLength >= BUCKET_CAPACITY) {
      console.log(`[Time: ${formatTime(now)}] ❌ Req ${requestId}: Queue Full! Request Rejected.`);
      return false;
    }

    // If there's room, drop the request into the bucket
    await client.lPush(BUCKET_KEY, requestId.toString());
    console.log(
      `[Time: ${formatTime(now)}] 📥 Req ${requestId}: Accepted into queue. (Queue size: ${currentQueueLength + 1}/${BUCKET_CAPACITY})`,
    );
    return true;
  }

  // --- 2. THE LEAK MECHANISM (Processing at a constant rate) ---
  function startLeaking() {
    const intervalId = setInterval(async () => {
      // Pull the oldest request from the bottom of the bucket
      const reqToProcess = await client.rPop(BUCKET_KEY);
      const now = Date.now();

      if (reqToProcess) {
        console.log(
          `[Time: ${formatTime(now)}] ⚙️ [LEAK] ─── Processing Req ${reqToProcess} steadily. ───`,
        );
      } else {
        console.log(`[Time: ${formatTime(now)}] ⚙️ [LEAK] Queue is empty. Idle...`);
      }
    }, LEAK_RATE_MS);

    return intervalId;
  }

  // --- TEST SIMULATION ---
  // Start the leak processing in the background
  const leakInterval = startLeaking();

  console.log("🚀 Starting simulation. Sending 10 requests spaced 2 seconds apart...");
  console.log("(Watch the Redis GUI to see the queue grow because inflow is faster than outflow!)\n");

  for (let i = 1; i <= 10; i++) {
    await handleIncomingRequest(i);
    // Spaced out by 2 seconds (2000ms)
    await new Promise((r) => setTimeout(r, 2000));
  }

  // Let it run for 60 seconds (60000ms) so you can watch it leak out steadily
  console.log(
    "\n⏳ Inflow stopped. Waiting 60 seconds to watch the queue drain at a steady leak rate...",
  );
  await new Promise((r) => setTimeout(r, 60000));

  // Cleanup
  clearInterval(leakInterval);
  //   await client.del(BUCKET_KEY);
  await client.quit();
}

run().catch(console.error);
