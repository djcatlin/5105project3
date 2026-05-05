# Question 3A - Fault Tolerance Plan

Our system uses a Docker-only architecture with a centralized controller, a scalable service layer, and a primary-backup replication strategy.

### How failures are detected
-  The controller sends periodic heartbeat pings to each storage replica every 2 seconds via gRPC. If a replica fails to respond to 3 consecutive heartbeats, the controller marks that replica as dead and removes it from the active replica set.
### What happens when a node dies
-  If a backup replica dies, the system continues operating normally since all reads and writes go through primary. The controller logs the failure and begins spinning up a replacement container. If the primary replica dies, the controller promotes one of the surviving backups to primary. The new primary already has a full copy of the data since the previous primary propagated all the writes to it. The controller updates its internal routing metadata so that service nodes direct subsequent requests to new primary.
### How data is recovered or maintained
- When a replacement replica is spun up (as a new Docker container), it performs a full state transfer from the current primary before joining active set. The primary streams all commited items to new replica. The new replica is not added until transfer is complete. We can use the "version" field on each item to esnure replacement replica has latest state and to detect inconsistencies.


# Question 3B - Evaluation Plan

### What performance metrics will you track?
- We will track:
- Request latency (average) for operations: CreateItem, GetItem, SearchItems, UpdateItem, PlaceBid
- Throughput (requests per second) under increasing load
- Recovery time: how long for system to resume normal operation after replica failure
- Autoscaling response time: how quickly new service nodes are added when demand increases
### How will you test many clients at once?
- We will write a Python load generator that spawns multiple concurrent gRPC client threads. Each thread will send a mix of read-heavy and write traffic to simulate realistic marketplace behavior. We will ramp from 10 to 100 concurrent clients to observe how latency and throughput can change.
### How will you test scaling?
- We will define a request-rate threshold as the autoscaling trigger. That way the controller monitors load and spins up additional service-layer Docker containers when demand increases. During evaluation, we will increase client load in bursts and observe whether the system adds service containers in response. We will also measure whether latency stabilizes after scale-up. For scale-down, we will reduce load and verify the system appropriately removes service containers without dropping active requests.
