import asyncio
import random
import time
import enum
from dataclasses import dataclass
from collections import defaultdict

# --- Configuration ---

# Network Conditions
MESSAGE_LOSS_RATE = 0.3  # 30% chance of any message being lost
MESSAGE_DELAY_RATE = 0.5  # 50% chance of a message being delayed
MAX_DELAY_SECONDS = 2.0  # Max delay for a message

# Policy Configuration
REMINDER_TIMEOUT = 5.0  # Patient waits 5 seconds before sending a reminder
SIMULATION_DURATION = 20.0  # How long to run the simulation

# "Dataset" of complaints
PATIENT_COMPLAINTS = [
    "headache",
    "cough",
    "sore_throat",
    "dizziness",
    "back_pain",
]


# --- Policy Definitions ---

class PolicyMode(enum.Enum):
    NAIVE = 1  # Fire and forget, no fault tolerance
    POLICY_REMIND = 2  # Patient reminds Doctor (Retry)
    POLICY_CHECKPOINT_CONTINUE = 3  # Doctor checkpoints to Patient, Patient can Continue to Pharmacist


@dataclass
class Complaint:
    sID: str  # Session ID
    symptom: str


@dataclass
class Prescription:
    sID: str
    symptom: str
    Rx: str  # The prescription medicine


@dataclass
class FilledRx:
    sID: str
    Rx: str
    done: bool


# --- Fault-Tolerance Messages (from the paper) ---

@dataclass
class ComplaintReminder:
    """Patient to Doctor: 'Hey, I'm still sick!'"""
    sID: str
    symptom: str


@dataclass
class FwdPrescription:
    """Patient to Pharmacist: 'Doctor sent this, please fill it.' (Continue)"""
    sID: str
    prescription: Prescription


class UnreliableNetwork:
    """Simulates an unreliable, asynchronous message transport."""
    def __init__(self, loss_rate=0.0, delay_rate=0.0, max_delay=1.0):
        self.loss_rate = loss_rate
        self.delay_rate = delay_rate
        self.max_delay = max_delay
        print(f"[Network] Initialized with {loss_rate * 100}% loss, {delay_rate * 100}% delay.")

    async def send(self, sender: 'Agent', recipient: 'Agent', message: object):
        """Simulates sending a message that might be lost or delayed."""

        # 1. Simulate Message Loss
        if random.random() < self.loss_rate:
            print(
                f"    [Network] 💧 DROPPED {message.__class__.__name__} from {sender.name} to {recipient.name} (sID: {message.sID})")
            return

        # 2. Simulate Message Delay
        if random.random() < self.delay_rate:
            delay = random.uniform(0.1, self.max_delay)
            print(f"    [Network] 🐢 DELAYED {message.__class__.__name__} from {sender.name} (by {delay:.1f}s)")
            await asyncio.sleep(delay)

        # 3. Deliver Message
        await recipient.inbox.put((sender.name, message))


class Agent:
    """Base class for our multi-agent system."""

    def __init__(self, name: str, network: UnreliableNetwork, policy_mode: PolicyMode):
        self.name = name
        self.network = network
        self.policy_mode = policy_mode
        self.inbox = asyncio.Queue()
        self._running = True

    async def send(self, recipient: 'Agent', message: object):
        """Helper to send a message via the network."""
        print(f"[{self.name}] 📨 Sending {message.__class__.__name__} to {recipient.name} (sID: {message.sID})")
        await self.network.send(self, recipient, message)

    async def run(self):
        """Main agent loop: processes messages from the inbox."""
        print(f"[{self.name}] Agent running...")
        while self._running:
            try:
                # Wait for a message or a timeout to check policies
                sender, message = await asyncio.wait_for(self.inbox.get(), timeout=1.0)
                await self._handle_message(sender, message)
            except asyncio.TimeoutError:
                pass
            except Exception as e:
                print(f"[{self.name}] Error in run loop: {e}")

    async def _handle_message(self, sender: str, message: object):
        """To be implemented by subclasses."""
        raise NotImplementedError

    def stop(self):
        self._running = False


class Patient(Agent):
    """The Patient agent. Initiates complaints and enacts fault-tolerance."""

    @dataclass
    class ComplaintStatus:
        symptom: str
        status: str = "PENDING"  # PENDING, CHECKPOINTED, DONE
        sent_time: float = 0.0
        last_reminder_time: float = 0.0
        prescription_copy: Prescription = None

    def __init__(self, name: str, network: UnreliableNetwork, policy_mode: PolicyMode, complaints: list,
                 doctor: 'Doctor', pharmacist: 'Pharmacist'):
        super().__init__(name, network, policy_mode)
        self.complaints_to_send = complaints
        self.doctor = doctor
        self.pharmacist = pharmacist
        self.complaint_state = {}  # sID -> ComplaintStatus

    async def run(self):
        """Extends base run to add proactive behaviors."""
        print(f"[{self.name}] Agent running with policy: {self.policy_mode.name}")

        # Start proactive tasks
        asyncio.create_task(self._start_complaints())

        if self.policy_mode != PolicyMode.NAIVE:
            asyncio.create_task(self._check_expectations_loop())

        await super().run()

    async def _start_complaints(self):
        """Proactively sends all initial complaints one by one."""
        for i, symptom in enumerate(self.complaints_to_send):
            await asyncio.sleep(random.uniform(0.5, 1.5))  # Stagger complaints
            sID = f"complaint-{i + 1}"

            self.complaint_state[sID] = self.ComplaintStatus(
                symptom=symptom,
                sent_time=time.time()
            )

            msg = Complaint(sID=sID, symptom=symptom)
            await self.send(self.doctor, msg)

    async def _check_expectations_loop(self):
        """
        The core of the Mandrake policy.
        A background task that checks if expectations are met.
        """
        while self._running:
            await asyncio.sleep(1.0)  # Check every second
            now = time.time()

            for sID, state in self.complaint_state.items():
                if state.status == "DONE":
                    continue

                time_since_last_action = now - (state.last_reminder_time or state.sent_time)

                if time_since_last_action < REMINDER_TIMEOUT:
                    continue

                # --- Apply Policy ---
                state.last_reminder_time = now

                if self.policy_mode == PolicyMode.POLICY_REMIND:
                    # Policy 1: Always remind the Doctor
                    print(f"[{self.name}] ⏰ TIMEOUT for {sID}. Reminding Doctor.")
                    await self.send(self.doctor, ComplaintReminder(sID=sID, symptom=state.symptom))

                elif self.policy_mode == PolicyMode.POLICY_CHECKPOINT_CONTINUE:
                    # Policy 2: Smarter reminder
                    if state.status == "PENDING":
                        # We have no checkpoint, so remind the Doctor
                        print(f"[{self.name}] ⏰ TIMEOUT for {sID} (Pending). Reminding Doctor.")
                        await self.send(self.doctor, ComplaintReminder(sID=sID, symptom=state.symptom))

                    elif state.status == "CHECKPOINTED":
                        # We have a checkpoint! The Doctor did their job.
                        # The fault must be with the Pharmacist.
                        # We "Continue" the protocol by forwarding our copy.
                        print(f"[{self.name}] ⏰ TIMEOUT for {sID} (Checkpointed). 'Continuing' to Pharmacist.")
                        await self.send(self.pharmacist, FwdPrescription(sID=sID, prescription=state.prescription_copy))

    async def _handle_message(self, sender: str, message: object):
        """Handles replies from the Doctor or Pharmacist."""
        sID = message.sID
        if sID not in self.complaint_state:
            print(f"[{self.name}] ❓ Received message for unknown sID: {sID}")
            return

        state = self.complaint_state[sID]

        if isinstance(message, FilledRx):
            if state.status != "DONE":
                print(f"[{self.name}] ✅ TREATMENT COMPLETE for {sID} ({state.symptom})! Received {message.Rx}.")
                state.status = "DONE"

        elif isinstance(message, Prescription) and self.policy_mode == PolicyMode.POLICY_CHECKPOINT_CONTINUE:
            if state.status == "PENDING":
                print(f"[{self.name}] ℹ️ CHECKPOINT received for {sID} ({state.symptom}). Now waiting for Pharmacist.")
                state.status = "CHECKPOINTED"
                state.prescription_copy = message

        else:
            print(f"[{self.name}] ❓ Received unexpected message type: {message.__class__.__name__}")

    def print_summary(self):
        print("\n--- Simulation Summary (Patient) ---")
        total = len(self.complaint_state)
        done = sum(1 for s in self.complaint_state.values() if s.status == "DONE")
        pending = sum(1 for s in self.complaint_state.values() if s.status == "PENDING")
        checkpointed = sum(1 for s in self.complaint_state.values() if s.status == "CHECKPOINTED")

        print(f"Policy Mode: {self.policy_mode.name}")
        print(f"Total Complaints: {total}")
        print(f"  ✅ Completed:    {done}")
        print(f"  ⌛ Pending:      {pending}")
        print(f"  ℹ️ Checkpointed: {checkpointed}")
        print("------------------------------------")


class Doctor(Agent):
    """The Doctor agent. Receives Complaints, sends Prescriptions."""

    def __init__(self, name: str, network: UnreliableNetwork, policy_mode: PolicyMode, patient: 'Patient',
                 pharmacist: 'Pharmacist'):
        super().__init__(name, network, policy_mode)
        self.patient = patient
        self.pharmacist = pharmacist
        self.treated_complaints = {}  # sID -> Prescription

    async def _handle_message(self, sender: str, message: object):
        sID = message.sID

        if isinstance(message, Complaint) or isinstance(message, ComplaintReminder):
            if isinstance(message, ComplaintReminder):
                print(f"[{self.name}] REMINDER received for {sID}")

            if sID in self.treated_complaints:
                # We've already treated this, must be a reminder
                prescription = self.treated_complaints[sID]
            else:
                # New complaint
                print(f"[{self.name}] 🩺 Treating {sID} ({message.symptom})")
                prescription = Prescription(sID=sID, symptom=message.symptom, Rx=f"MandrakeMeds-for-{message.symptom}")
                self.treated_complaints[sID] = prescription

            # Send prescription to Pharmacist
            await self.send(self.pharmacist, prescription)

            # --- Mandrake Checkpoint Policy ---
            if self.policy_mode == PolicyMode.POLICY_CHECKPOINT_CONTINUE:
                # Also send a copy to the Patient as a checkpoint
                await self.send(self.patient, prescription)
        else:
            print(f"[{self.name}] ❓ Received unexpected message type: {message.__class__.__name__}")


class Pharmacist(Agent):
    """The Pharmacist agent. Fills prescriptions."""

    def __init__(self, name: str, network: UnreliableNetwork, policy_mode: PolicyMode, patient: 'Patient'):
        super().__init__(name, network, policy_mode)
        self.patient = patient
        self.filled_prescriptions = set()  # sID

    async def _handle_message(self, sender: str, message: object):
        if isinstance(message, Prescription) or isinstance(message, FwdPrescription):

            # The "Continue" message (FwdPrescription) is handled
            # identically to the original Prescription.

            if isinstance(message, FwdPrescription):
                print(f"[{self.name}] ℹ️ Received FORWARDED prescription from Patient for {message.sID}")
                prescription = message.prescription
            else:
                print(f"[{self.name}] 🧾 Received prescription from Doctor for {message.sID}")
                prescription = message

            sID = prescription.sID
            if sID in self.filled_prescriptions:
                # Idempotency: We've already filled this. Just resend the notification.
                print(f"[{self.name}] 🧾 Resending FilledRx for {sID}")
            else:
                # New prescription to fill
                print(f"[{self.name}] 💊 Filling {sID} ({prescription.Rx})")
                self.filled_prescriptions.add(sID)

            # Send notification to Patient
            filled_msg = FilledRx(sID=sID, Rx=prescription.Rx, done=True)
            await self.send(self.patient, filled_msg)
        else:
            print(f"[{self.name}] ❓ Received unexpected message type: {message.__class__.__name__}")


async def main():
    # --- CHOOSE YOUR EXPERIMENT ---
    # Try changing this mode and the MESSAGE_LOSS_RATE at the top

    # policy_mode = PolicyMode.NAIVE
    policy_mode = PolicyMode.POLICY_REMIND
    # policy_mode = PolicyMode.POLICY_CHECKPOINT_CONTINUE

    # --- Setup ---
    network = UnreliableNetwork(
        loss_rate=MESSAGE_LOSS_RATE,
        delay_rate=MESSAGE_DELAY_RATE,
        max_delay=MAX_DELAY_SECONDS
    )

    # Create agents
    # We create them first...
    patient = Patient("Patient", network, policy_mode, PATIENT_COMPLAINTS, None, None)
    doctor = Doctor("Doctor", network, policy_mode, patient, None)
    pharmacist = Pharmacist("Pharmacist", network, policy_mode, patient)

    # ...then link them.
    patient.doctor = doctor
    patient.pharmacist = pharmacist
    doctor.pharmacist = pharmacist

    print(f"--- Starting {SIMULATION_DURATION}s Simulation (Policy: {policy_mode.name}) ---")

    # Start agent tasks
    tasks = [
        asyncio.create_task(patient.run()),
        asyncio.create_task(doctor.run()),
        asyncio.create_task(pharmacist.run())
    ]

    # Run simulation
    await asyncio.sleep(SIMULATION_DURATION)

    # Stop agents and simulation
    for agent in [patient, doctor, pharmacist]:
        agent.stop()
    for task in tasks:
        task.cancel()

    print(f"\n--- Simulation Over ---")

    # Print final summary
    patient.print_summary()


if __name__ == "__main__":
    asyncio.run(main())