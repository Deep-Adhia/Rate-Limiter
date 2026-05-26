const redis = require("redis");

async function run() {
  const client = redis.createClient();
  await client.connect();

  const USER_ID = "exploiter_joe";
  const LIMIT = 5; // Max requests allowed in any rolling window
  const WINDOW_SIZE_MS = 10000; // 10-second rolling window

  // Helper function to format timestamp into human-readable clock time
  function formatTime(timestampMs) {
    const date = new Date(timestampMs);
    const hours = date.getHours().toString().padStart(2, '0');
    const minutes = date.getMinutes().toString().padStart(2, '0');
    const seconds = date.getSeconds().toString().padStart(2, '0');
    const milliseconds = date.getMilliseconds().toString().padStart(3, '0');
    return `${hours}:${minutes}:${seconds}.${milliseconds}`;
  }

  // --- SLIDING WINDOW ALGORITHM ---
  async function checkSlidingWindow(user) {
    const key = `ratelimit:slidingwindow:${user}`;
    const now = Date.now();
    const clearBefore = now - WINDOW_SIZE_MS;
    
    // We append a random ID so that multiple requests in the exact same millisecond have unique values
    const uniqueValue = `${now}:${Math.random().toString(36).substring(2, 6)}`;

    // Use a multi-transaction to perform commands atomically
    const transaction = client.multi();

    // 1. Remove all requests older than the current sliding window
    transaction.zRemRangeByScore(key, "-inf", clearBefore);

    // 2. Add current request timestamp
    transaction.zAdd(key, { score: now, value: uniqueValue });

    // 3. Count total active requests in the current rolling window
    transaction.zCard(key);

    // 4. Set key expiration to save memory
    transaction.expire(key, Math.ceil(WINDOW_SIZE_MS / 1000) * 2);

    const [removedCount, addedCount, activeRequestsCount] = await transaction.exec();

    console.log(`\n🔍 [Rate Limit Check] Time: ${formatTime(now)}`);
    console.log(`   ├─ Removing requests older than: ${formatTime(clearBefore)}`);
    
    // Get all timestamps currently active in the window for human-readable logging
    const activeMembers = await client.zRangeWithScores(key, 0, -1);
    const readableTimestamps = activeMembers.map(m => formatTime(m.score));
    console.log(`   ├─ Active requests in rolling window: [${readableTimestamps.join(", ")}]`);

    if (activeRequestsCount > LIMIT) {
      console.log(`   └─ ❌ Request DENIED! (${activeRequestsCount}/${LIMIT} requests in last 10 seconds)`);
      // Since it was denied, remove the request we just added so it doesn't penalize the user
      await client.zRem(key, uniqueValue);
      return false;
    }

    console.log(`   └─ ✅ Request ALLOWED! (${activeRequestsCount}/${LIMIT} requests in last 10 seconds)`);
    return true;
  }

  // --- THE EXPLOIT SIMULATION ---
  console.log("🛠️ Preparing the Sliding Window Test...");
  console.log("We will attempt the exact same boundary exploit (10 requests in 2 seconds) and watch it get blocked!\n");

  // Wait to align window boundary just like before
  let timeRemainingInWindow = WINDOW_SIZE_MS - (Date.now() % WINDOW_SIZE_MS);
  if (timeRemainingInWindow > 1500) {
    const waitTime = timeRemainingInWindow - 1500;
    console.log(`⏳ Aligning timing... Waiting ${waitTime}ms to reach the boundary...`);
    await new Promise((r) => setTimeout(r, waitTime));
  }

  console.log("\n🔥 [BURST 1] Flooding 5 requests at the END of the first window segment...");
  for (let i = 1; i <= 5; i++) {
    await checkSlidingWindow(USER_ID);
    await new Promise((r) => setTimeout(r, 50)); // Tiny spacing
  }

  // Wait 2 seconds (Crossing the virtual boundary)
  console.log(`\n⏳ Waiting 2 seconds to cross the "boundary"... (Current Time: ${formatTime(Date.now())})`);
  await new Promise((r) => setTimeout(r, 2000));

  console.log("\n🔥 [BURST 2] Attempting to flood 5 more requests in the next segment...");
  for (let i = 1; i <= 5; i++) {
    await checkSlidingWindow(USER_ID);
    await new Promise((r) => setTimeout(r, 50));
  }

  // Cleanup
  await client.del(`ratelimit:slidingwindow:${USER_ID}`);
  await client.quit();
}

run().catch(console.error);
