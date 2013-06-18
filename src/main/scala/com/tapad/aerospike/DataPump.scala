package com.tapad.aerospike

import com.aerospike.client.async.{AsyncClientPolicy, MaxCommandAction, AsyncClient}
import java.util.concurrent.atomic.AtomicInteger
import com.aerospike.client.policy.{ClientPolicy, WritePolicy, ScanPolicy}
import com.aerospike.client._
import java.util
import scala.collection.JavaConverters._
import com.aerospike.client.listener.WriteListener
import java.util.concurrent.Executors
import com.aerospike.client.AerospikeException.CommandRejected
import scala.annotation.tailrec

object DataPump {
  def main(args: Array[String]) {

    val sourceAddr = args(0)
    val destAddr = args(1)
    val namespace = args(2)

    val sourceClientPolicy = new AsyncClientPolicy

    val destClientPolicy = new AsyncClientPolicy
    destClientPolicy.asyncMaxCommandAction = MaxCommandAction.REJECT
    destClientPolicy.asyncMaxCommands = 1000
    destClientPolicy.asyncTaskThreadPool = Executors.newCachedThreadPool()

    val source = new AsyncClient(sourceClientPolicy, sourceAddr, 3000)
    val destination = new AsyncClient(destClientPolicy, destAddr, 3000)

    println("Copying all data from namespace %s from cluster at %s to %s...".format(namespace, sourceAddr, destAddr))

    val recordsMoved = new AtomicInteger()

    val scanPolicy = new ScanPolicy()
    scanPolicy.threadsPerNode = 4

    val writePolicy = new WritePolicy()

    var startTime = System.currentTimeMillis()
    val batchSize = 100000

    val writeListener = new WriteListener {
      def onFailure(exception: AerospikeException) {
        println("Error " + exception.toString)
      }

      def onSuccess(key: Key) {
        val count = recordsMoved.incrementAndGet()
        if (count % batchSize == 0) {
          val elapsed = System.currentTimeMillis() - startTime
          startTime = System.currentTimeMillis()
          println("Processed %(,d bins / records, %d ms, %.2f records / sec".format(
            count, elapsed, batchSize.toFloat / elapsed * 1000)
          )
        }
      }
    }
    source.scanAll(scanPolicy, namespace, "", new ScanCallback {
      def scanCallback(key: Key, record: Record) {
        val bins = new util.ArrayList[Bin]()
        val i = record.bins.entrySet().iterator()
        while (i.hasNext) {
          val e = i.next()
          bins.add(new Bin(e.getKey, e.getValue))
        }

        var sent = false
        while (!sent) {
          try {
            destination.put(writePolicy, writeListener, key, bins.asScala: _*)
            sent = true
          } catch {
            case e: CommandRejected =>
              println("Applying back-pressure...")
              Thread.sleep(10)
          }
        }

      }
    })
    println("Done, a total of %d records moved...".format(recordsMoved.get()))
  }
}
