const redis = require("redis");

// Helper to format timestamps
function formatTime(timestampMs) {
  const date = new Date(Number(timestampMs));
  const hours = date.getHours().toString().padStart(2, "0");
  const minutes = date.getMinutes().toString().padStart(2, "0");
  const seconds = date.getSeconds().toString().padStart(2, "0");
  const milliseconds = date.getMilliseconds().toString().padStart(3, "0");
  return `${hours}:${minutes}:${seconds}.${milliseconds}`;
}

async function run() {
  const client = redis.createClient();
  await client.connect();

  const STREAM_KEY = "orders:stream";
  const GROUP_NAME = "order-processors";

  // Clean up from previous runs
  try {
    await client.del(STREAM_KEY);
  } catch (e) {}

  // ============================================================
  // PHASE 1: CREATE THE STREAM AND CONSUMER GROUP
  // ============================================================
  console.log("═══════════════════════════════════════════════════");
  console.log("  PHASE 1: Setting up Stream & Consumer Group");
  console.log("═══════════════════════════════════════════════════\n");

  // Create the consumer group. The "$" means "only read NEW messages from now on"
  // mkStream: true creates the stream if it doesn't exist yet
  await client.xGroupCreate(STREAM_KEY, GROUP_NAME, "$", { MKSTREAM: true });
  console.log(
    `[Time: ${formatTime(Date.now())}] ✅ Created stream "${STREAM_KEY}" and consumer group "${GROUP_NAME}"\n`,
  );

  // ============================================================
  // PHASE 2: PRODUCE 5 ORDERS INTO THE STREAM
  // ============================================================
  console.log("═══════════════════════════════════════════════════");
  console.log("  PHASE 2: Producing 5 Orders");
  console.log("═══════════════════════════════════════════════════\n");

  const orders = [
    { item: "Pizza Margherita", customer: "Alice", amount: "12.99" },
    { item: "Chicken Burger", customer: "Bob", amount: "9.49" },
    { item: "Caesar Salad", customer: "Charlie", amount: "7.99" },
    { item: "Pasta Carbonara", customer: "Diana", amount: "14.50" },
    { item: "Sushi Platter", customer: "Eve", amount: "22.00" },
  ];

  const messageIds = [];
  for (const order of orders) {
    // XADD appends a message to the stream. "*" tells Redis to auto-generate a unique message ID
    const messageId = await client.xAdd(STREAM_KEY, "*", {
      item: order.item,
      customer: order.customer,
      amount: order.amount,
    });
    messageIds.push(messageId);
    console.log(
      `[Time: ${formatTime(Date.now())}] 📤 [PRODUCER] Added Order: "${order.item}" for ${order.customer} ($${order.amount}) | Message ID: ${messageId}`,
    );
    await new Promise((r) => setTimeout(r, 200));
  }

  console.log(`\n   └─ Total messages in stream: ${await client.xLen(STREAM_KEY)}\n`);

  // ============================================================
  // PHASE 3: CONSUMER GROUP PROCESSING (Load Balanced)
  // ============================================================
  console.log("═══════════════════════════════════════════════════");
  console.log("  PHASE 3: Two Workers Processing Orders");
  console.log("═══════════════════════════════════════════════════\n");

  // Worker function: reads from the stream as part of the consumer group
  async function processNextOrder(workerName) {
    // XREADGROUP: Read the next undelivered message for this consumer group
    // ">" means "give me messages that have never been delivered to any consumer in this group"
    const response = await client.xReadGroup(GROUP_NAME, workerName, [
      { key: STREAM_KEY, id: ">" },
    ], { COUNT: 1 });

    if (!response || response.length === 0) {
      console.log(
        `[Time: ${formatTime(Date.now())}] 💤 [${workerName}] No more orders to process.`,
      );
      return null;
    }

    const message = response[0].messages[0];
    const orderId = message.id;
    const orderData = message.message;

    console.log(
      `[Time: ${formatTime(Date.now())}] 📥 [${workerName}] Picked up Order "${orderData.item}" for ${orderData.customer} | ID: ${orderId}`,
    );

    return { orderId, orderData, workerName };
  }

  // Simulate Worker A processing 2 orders successfully
  for (let i = 0; i < 2; i++) {
    const job = await processNextOrder("Worker-A");
    if (job) {
      // Simulate processing time
      await new Promise((r) => setTimeout(r, 500));
      // XACK: Tell Redis "I have successfully processed this message"
      await client.xAck(STREAM_KEY, GROUP_NAME, job.orderId);
      console.log(
        `   └─ ✅ [${job.workerName}] Finished & Acknowledged Order "${job.orderData.item}" (ID: ${job.orderId})\n`,
      );
    }
  }

  // Simulate Worker B picking up an order BUT CRASHING before acknowledging
  console.log("─── Simulating Worker-B picking up an order and CRASHING ───\n");
  const crashedJob = await processNextOrder("Worker-B");
  if (crashedJob) {
    await new Promise((r) => setTimeout(r, 300));
    console.log(
      `   └─ 💥 [${crashedJob.workerName}] CRASHED while processing "${crashedJob.orderData.item}"! (No XACK sent!)\n`,
    );
    // Notice: We do NOT call xAck here! The message stays in the "Pending" list.
  }

  // Worker A continues and processes one more order normally
  const job4 = await processNextOrder("Worker-A");
  if (job4) {
    await new Promise((r) => setTimeout(r, 500));
    await client.xAck(STREAM_KEY, GROUP_NAME, job4.orderId);
    console.log(
      `   └─ ✅ [${job4.workerName}] Finished & Acknowledged Order "${job4.orderData.item}" (ID: ${job4.orderId})\n`,
    );
  }

  // ============================================================
  // PHASE 4: INSPECT THE PENDING LIST (Unacknowledged Messages)
  // ============================================================
  console.log("═══════════════════════════════════════════════════");
  console.log("  PHASE 4: Checking Pending (Unacknowledged) Messages");
  console.log("═══════════════════════════════════════════════════\n");

  // XPENDING: Shows messages that were delivered but never acknowledged
  const pending = await client.xPending(STREAM_KEY, GROUP_NAME);
  console.log(`[Time: ${formatTime(Date.now())}] 🔍 Pending messages in group "${GROUP_NAME}":`);
  console.log(`   └─ Total pending: ${pending.pending}`);
  if (pending.pending > 0) {
    console.log(`   └─ Smallest pending ID: ${pending.minMessageId}`);
    console.log(`   └─ Largest pending ID: ${pending.maxMessageId}`);
    console.log(`   └─ Consumers with pending messages:`);
    for (const consumer of pending.consumers) {
      console.log(`       └─ ${consumer.name}: ${consumer.deliveryCount} pending message(s)`);
    }
  }

  // ============================================================
  // PHASE 5: CRASH RECOVERY - Claim the lost message
  // ============================================================
  console.log("\n═══════════════════════════════════════════════════");
  console.log("  PHASE 5: Crash Recovery - Reclaiming Lost Order");
  console.log("═══════════════════════════════════════════════════\n");

  // XAUTOCLAIM: Automatically claim messages that have been pending for too long
  // "0-0" means start scanning from the very beginning of the pending list
  // minIdleTime: 0 means claim immediately (in production, you'd set this to e.g. 30000ms)
  const claimed = await client.xAutoClaim(STREAM_KEY, GROUP_NAME, "Worker-A", 0, "0-0");

  if (claimed.messages.length > 0) {
    for (const msg of claimed.messages) {
      console.log(
        `[Time: ${formatTime(Date.now())}] 🔄 [Worker-A] Reclaimed crashed order "${msg.message.item}" for ${msg.message.customer} | ID: ${msg.id}`,
      );
      // Process the reclaimed order
      await new Promise((r) => setTimeout(r, 500));
      await client.xAck(STREAM_KEY, GROUP_NAME, msg.id);
      console.log(
        `   └─ ✅ [Worker-A] Successfully re-processed & Acknowledged! Order "${msg.message.item}" is now complete.\n`,
      );
    }
  } else {
    console.log("   └─ No pending messages to reclaim.");
  }

  // Process the last remaining order
  const lastJob = await processNextOrder("Worker-A");
  if (lastJob) {
    await new Promise((r) => setTimeout(r, 500));
    await client.xAck(STREAM_KEY, GROUP_NAME, lastJob.orderId);
    console.log(
      `   └─ ✅ [${lastJob.workerName}] Finished & Acknowledged Order "${lastJob.orderData.item}" (ID: ${lastJob.orderId})\n`,
    );
  }

  // ============================================================
  // PHASE 6: FINAL STATE - Verify everything is clean
  // ============================================================
  console.log("═══════════════════════════════════════════════════");
  console.log("  PHASE 6: Final Verification");
  console.log("═══════════════════════════════════════════════════\n");

  const finalPending = await client.xPending(STREAM_KEY, GROUP_NAME);
  console.log(`[Time: ${formatTime(Date.now())}] 🔍 Final pending count: ${finalPending.pending}`);

  const streamLength = await client.xLen(STREAM_KEY);
  console.log(`   └─ Total messages in stream (permanent log): ${streamLength}`);

  // Read the entire stream history
  const allMessages = await client.xRange(STREAM_KEY, "-", "+");
  console.log(`   └─ Full stream history:`);
  for (const msg of allMessages) {
    console.log(
      `       └─ [${msg.id}] ${msg.message.item} for ${msg.message.customer} ($${msg.message.amount})`,
    );
  }

  console.log(`\n✅ All ${streamLength} orders processed successfully. Zero messages lost!`);

  // Cleanup
  await client.del(STREAM_KEY);
  await client.quit();
}

run().catch(console.error);
