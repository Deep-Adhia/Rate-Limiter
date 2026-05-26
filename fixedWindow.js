const redis = require("redis");

async function run() {
  const client = redis.createClient();
  await client.connect();

  const USER_ID = "exploiter_joe";
  const LIMIT = 5; // Max requests allowed per window
  const WINDOW_SIZE_MS = 10000; // 10-second window

  // Helper function to format timestamp into human-readable clock time
  function formatTime(timestampMs) {
    const date = new Date(Number(timestampMs));
    const hours = date.getHours().toString().padStart(2, '0');
    const minutes = date.getMinutes().toString().padStart(2, '0');
    const seconds = date.getSeconds().toString().padStart(2, '0');
    const milliseconds = date.getMilliseconds().toString().padStart(3, '0');
    return `${hours}:${minutes}:${seconds}.${milliseconds}`;
  }

  // --- FIXED WINDOW ALGORITHM ---
  async function checkFixedWindow(user) {
    const now = Date.now();
    // Calculate which fixed window we are currently in (e.g., window 18023)
    const currentWindowId = Math.floor(now / WINDOW_SIZE_MS);
    const key = `ratelimit:fixedwindow:${user}:${currentWindowId}`;

    const windowStart = currentWindowId * WINDOW_SIZE_MS;
    const windowEnd = windowStart + WINDOW_SIZE_MS;

    // Increment request count in Redis atomically
    const currentRequests = await client.incr(key);

    // If it's the first request in this window, set expiration so Redis cleans it up
    if (currentRequests === 1) {
      await client.expire(key, Math.ceil(WINDOW_SIZE_MS / 1000) * 2); // Expire in 20s
    }

    if (currentRequests > LIMIT) {
      console.log(`[Time: ${formatTime(now)}] ❌ Request denied! (Count: ${currentRequests}/${LIMIT} in Window [${formatTime(windowStart)} - ${formatTime(windowEnd)}])`);
      return false;
    }

    console.log(`[Time: ${formatTime(now)}] ✅ Request allowed! (Count: ${currentRequests}/${LIMIT} in Window [${formatTime(windowStart)} - ${formatTime(windowEnd)}])`);
    return true;
  }

  // --- THE EXPLOIT SIMULATION ---
  console.log("🛠️ Preparing the Time-Window Exploit...");

  // 1. We need to align our timing so we hit the very end of the current window
  let timeRemainingInWindow = WINDOW_SIZE_MS - (Date.now() % WINDOW_SIZE_MS);
  
  // Wait until there is only 1.5 seconds left in the current window
  if (timeRemainingInWindow > 1500) {
    const waitTime = timeRemainingInWindow - 1500;
    console.log(`⏳ Waiting ${waitTime}ms to get near the end of the window...`);
    await new Promise((r) => setTimeout(r, waitTime));
  }

  console.log("\n🔥 [BURST 1] Flooding 5 requests at the END of Window A...");
  for (let i = 1; i <= 5; i++) {
    await checkFixedWindow(USER_ID);
  }

  // 2. Wait 2 seconds (Crossing the boundary into Window B)
  console.log(`\n⏳ Waiting 2 seconds to cross the boundary... (Current Time: ${formatTime(Date.now())})`);
  await new Promise((r) => setTimeout(r, 2000));

  console.log("\n🔥 [BURST 2] Flooding 5 requests at the START of Window B...");
  for (let i = 1; i <= 5; i++) {
    await checkFixedWindow(USER_ID);
  }

  console.log("\n🚨 [EXPLOIT STATUS]: Successfully processed 10 requests in 2 seconds!");
  console.log("Even though the strict limit was supposed to be 5 requests per 10 seconds!");

  // Cleanup
  const currentWindowId = Math.floor(Date.now() / WINDOW_SIZE_MS);
  await client.del(`ratelimit:fixedwindow:${USER_ID}:${currentWindowId}`);
  await client.del(`ratelimit:fixedwindow:${USER_ID}:${currentWindowId - 1}`);
  await client.quit();
}

run().catch(console.error);
