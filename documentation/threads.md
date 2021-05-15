# Threads

In order to allow quick referencing of the elements involved in the synchronization operations,
we apply naming conventions.

# Class and methods

* All thread function names start with the prefix `__thread_`.
* All classes that starts threads implement a method called `__start_theads`.

# Threads 

* Node (`node.Node`):
   * Message listener: wait for messages from other nodes.
   * CRON: perform periodic node maintenance tasks.
* Message supervisor (`message_supervisor.message_supervisor.MessageSupervisor`):
   * Message cleaner: periodically look for unanswered messages, and perform
     appropriate actions on these messages by calling a cleaner function, executed 
     as a thread. 
   * Cleaner function: see above description for the message cleaner. 
* Routing table (`routing_table.RoutingTable`):
   * Node inserter: periodically scans the insertion queues in
     order to find nodes that are waiting for potential insertion into k-buckets.
     
# Resources and locks

* All locks have names that begin with `__lock_`.
* All shared variables have names that begin with `__shared_`.

# Using RLock

RLocks (for _re-entrant_) must be used if the following situation may occur:

![](images/rlock.png)

