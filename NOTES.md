Think About It: Why does Spark deliberately delay execution (lazy evaluation) instead of running each transformation immediately like a normal Python function call?

Apache Spark deliberately delays execution using lazy evaluation so it can see your entire workflow before doing any heavy lifting. Instead of running line-by-line, Spark builds a blueprint—a Directed Acyclic Graph (DAG)—of all your transformations and waits until an action (like .count() or .save()) forces it to run.

Think About It: If you have a 10 GB file and only 8 GB of RAM on one machine, what specifically breaks if you try to
load the whole thing into a plain Python list?
If you try to load a 10 GB file into a plain Python list on a machine with 8 GB of RAM, your program will crash with a MemoryError—or the operating system will abruptly kill the process (an OOM Kill).