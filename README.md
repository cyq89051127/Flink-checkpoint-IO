# Flink-checkpoint-IO

This project shows some Demo of State Processor API which is new introduced in Flink 1.9. The key idea to access flink checkpoint is to start an flink app to read

The classes in package of com.cyq.chk.app are for running flink app and checkpointing.The classes In package of com.cyq.chk.read are for reading from checkpoint.

| WordCountWithoutWindow | snapshotting the reducer state with a key of type `String` |
| ---- | ---- |
|   ReadWithStringKey   |   reading checkpoint of the reducer state with a key of type `String`   |
| ReadWithTupleStringKey | reading checkpoint of the reducer state with a key of type `Tuple[String]` |
