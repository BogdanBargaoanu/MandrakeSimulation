# Mandrake: Fault-Tolerance Simulation

This project is a Python asyncio simulation of the fault-tolerance concepts from the "Mandrake: Multi-Agent Systems" paper.

It demonstrates the difference between a system without fault tolerance and a system with Mandrake's application-level policies.

## Overview

This simulation creates three agents: a **Patient**, a **Doctor**, and a **Pharmacist**, just like in the paper's healthcare scenario.

These agents communicate asynchronously over a simulated `UnreliableNetwork` that intentionally loses messages (e.g., a 30% message loss rate).

The "dataset" is a simple list of complaints (e.g., 'headache', 'cough') that the Patient agent will try to get treated.

## Core Concepts Demonstrated

- **Multi-Agent System**: The Patient, Doctor, and Pharmacist classes all run independently and concurrently.
- **Unreliable Messaging**: The `UnreliableNetwork` class randomly "drops" messages to simulate network faults.
- **Application-Level Fault Tolerance**: Instead of relying on TCP (which we're not using), the agents themselves are responsible for recovering from message loss.

## The Policies (The "Mandrake" Part)

The most important part of the simulation is the `POLICY_MODE` you can set.

### 1. `PolicyMode.NAIVE`

**What it does**: This is the "unreliable" mode. The Patient sends a Complaint and just... hopes. The Doctor sends a Prescription to the Pharmacist and hopes.

**Expected Outcome**: With a 30% message loss rate, most complaints will fail. The Patient will never get a `FilledRx` because either the Complaint or the Prescription was lost.

### 2. `PolicyMode.POLICY_REMIND` (Retry)

**What it does**: This implements the "Remind" pattern. The Patient agent now has an internal timer. If it doesn't receive a `FilledRx` within a `REMINDER_TIMEOUT` (e.g., 5 seconds), it assumes a fault and sends a `ComplaintReminder` to the Doctor.

**Expected Outcome**: Much better! The system can now recover from a lost Complaint or a lost Prescription. The Doctor, upon receiving a reminder, will resend the Prescription. This will result in many more "DONE" complaints.

### 3. `PolicyMode.POLICY_CHECKPOINT_CONTINUE` (The Full Mandrake Model)

**What it does**: This is the most advanced policy from the paper.

- **Checkpoint**: When the Doctor sends the Prescription to the Pharmacist, it also sends a copy (a checkpoint) to the Patient.
- **Continue**: The Patient's policy is now smarter.
  - If it hasn't heard anything, it "Reminds" the Doctor.
  - If it has received the Prescription checkpoint (but not the `FilledRx`), it knows the Doctor has done their job. The fault must be with the Pharmacist. The Patient now "Continues" the protocol by forwarding its Prescription copy directly to the Pharmacist.

**Expected Outcome**: The most robust system. This model can recover efficiently even if the Doctor is slow or the initial Prescription is lost. The Patient actively participates in its own recovery.

## How to Run

You just need Python 3.7+ (for asyncio and dataclasses).

1. Save the `mandrake_simulation.py` file.
2. Run it from your terminal:

```bash
python3 mandrake_simulation.py
```

## Change the Policy:
Change the `policy_mode` variable to `PolicyMode.NAIVE`, `PolicyMode.POLICY_REMIND`, or `PolicyMode.POLICY_CHECKPOINT_CONTINUE`. Run the script for each mode and compare the final summary.

## Change the Network Conditions:
At the top of the file, change the global constants:

`MESSAGE_LOSS_RATE`: Set this to `0.0` to see the "happy path" where nothing fails. Set it to 0.5 (50%) to see how the policies handle extreme packet loss.

`MESSAGE_DELAY_RATE`: Set this to `0.7` (70%) to simulate a slow, laggy network.

`REMINDER_TIMEOUT`: Decrease this to make the `Patient` more "impatient" and send reminders faster.
