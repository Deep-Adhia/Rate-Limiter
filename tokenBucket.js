const redis = require("redis");

async function run() {
  // Connect to your local Redis instance
  const client = redis.createClient();
  client.on("error", (err) => console.error("Redis Client Error", err));
  await client.connect();

  const userId = "user_token_demo";

  // Configuration
  const MAX_TOKENS = 5; // Max burst capacity
  const REFILL_RATE_PER_SEC = 1; // How many tokens generated per second
  const REFILL_RATE_PER_MS = REFILL_RATE_PER_SEC / 1000;

  // Helper function to format timestamp into human-readable clock time
  function formatTime(timestampMs) {
    const date = new Date(Number(timestampMs));
    const hours = date.getHours().toString().padStart(2, '0');
    const minutes = date.getMinutes().toString().padStart(2, '0');
    const seconds = date.getSeconds().toString().padStart(2, '0');
    const milliseconds = date.getMilliseconds().toString().padStart(3, '0');
    return `${hours}:${minutes}:${seconds}.${milliseconds}`;
  }

  async function checkTokenBucket(user) {
    const key = `ratelimit:tokenbucket:${user}`;
    const now = Date.now();

    console.log(`🔑 [Rate Limiter] Time: ${formatTime(now)} | User ID: "${user}" | Redis Key: "${key}"`);

    // 1. Fetch current bucket state from Redis Hash
    const data = await client.hGetAll(key);
    if (Object.keys(data).length > 0) {
      console.log(`   └─ Raw Redis Data: { tokens: '${data.tokens}', lastRefilled: '${data.lastRefilled}' (${formatTime(data.lastRefilled)}) }`);
    } else {
      console.log(`   └─ Raw Redis Data:`, data);
    }
    let tokens;
    let lastRefilled;

    if (Object.keys(data).length === 0) {
      // First request ever: start with a full bucket
      console.log(
        `   └─ No Redis record found. Initializing new bucket with ${MAX_TOKENS} tokens.`,
      );
      tokens = MAX_TOKENS;
      lastRefilled = now;
    } else {
      tokens = parseFloat(data.tokens);
      lastRefilled = parseInt(data.lastRefilled);

      // 2. "Lazy Refill" Calculation
      const elapsedMs = now - lastRefilled;
      const tokensToAdd = elapsedMs * REFILL_RATE_PER_MS;

      // Add tokens but never exceed the maximum bucket capacity
      const updatedTokens = Math.min(MAX_TOKENS, tokens + tokensToAdd);
      console.log(
        `   └─ Before Refill: ${tokens.toFixed(4)} tokens | Last Refilled: ${formatTime(lastRefilled)}`,
      );
      console.log(
        `   └─ Refill: +${tokensToAdd.toFixed(4)} tokens (Elapsed time: ${elapsedMs}ms)`,
      );
      console.log(
        `   └─ After Refill (capped at ${MAX_TOKENS}): ${updatedTokens.toFixed(4)} tokens`,
      );

      tokens = updatedTokens;
      lastRefilled = now;
    }

    // 3. Evaluate if user has enough tokens
    if (tokens >= 1) {
      tokens -= 1; // Consume 1 token
      console.log(
        `   └─ Action: ✅ Allowed (Consumed 1 token. Remaining: ${tokens.toFixed(4)})`,
      );

      // Update Redis with new state
      await client.hSet(key, {
        tokens: tokens.toString(),
        lastRefilled: lastRefilled.toString(),
      });
      return { allowed: true, remainingTokens: Math.floor(tokens) };
    } else {
      // No tokens left! Request denied. Still update time to save the partially accumulated tokens.
      console.log(
        `   └─ Action: ❌ Denied (Not enough tokens. Remaining: ${tokens.toFixed(4)})`,
      );
      await client.hSet(key, {
        tokens: tokens.toString(),
        lastRefilled: lastRefilled.toString(),
      });
      return { allowed: false, remainingTokens: Math.floor(tokens) };
    }
  }

  // --- TEST SIMULATION ---
  console.log("🚀 Simulating 7 rapid requests (Burst Mode)...");
  for (let i = 1; i <= 7; i++) {
    console.log(`\n--- [Req ${i}] ---`);
    const result = await checkTokenBucket(userId);
    console.log(
      `   └─ Outcome: ${result.allowed ? "✅ Allowed" : "❌ Denied"} | Integer Tokens left: ${result.remainingTokens}`,
    );
    await new Promise((r) => setTimeout(r, 100)); // 100ms delay
  }

  console.log("\n⏳ Waiting 3.5 seconds for the bucket to refill...");
  await new Promise((r) => setTimeout(r, 3500));

  console.log("\n🚀 Sending 2 more requests after waiting...");
  for (let i = 1; i <= 2; i++) {
    console.log(`\n--- [Post-Wait Req ${i}] ---`);
    const result = await checkTokenBucket(userId);
    console.log(
      `   └─ Outcome: ${result.allowed ? "✅ Allowed" : "❌ Denied"} | Integer Tokens left: ${result.remainingTokens}`,
    );
  }

  // Clean up and close connection
  //   await client.del(`ratelimit:tokenbucket:${userId}`);
  await client.quit();
}

run().catch(console.error);
