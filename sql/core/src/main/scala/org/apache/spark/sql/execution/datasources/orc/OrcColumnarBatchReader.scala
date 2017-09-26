/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.orc

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.orc._
import org.apache.orc.TypeDescription.Category.CHAR
import org.apache.orc.mapred.OrcInputFormat
import org.apache.orc.storage.ql.exec.vector._

import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.execution.vectorized.{ColumnarBatch, ColumnVectorUtils}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.collection.BitSet


/**
 * To support vectorization in WholeStageCodeGen, this reader returns ColumnarBatch.
 */
private[orc] class OrcColumnarBatchReader extends RecordReader[Void, ColumnarBatch] with Logging {
  import OrcColumnarBatchReader._

  /**
   * ORC Data Schema.
   */
  private[this] var schema: TypeDescription = _

  /**
   * Vectorized Row Batch.
   */
  private[this] var batch: VectorizedRowBatch = _

  /**
   * Record reader from row batch.
   */
  private[this] var rows: org.apache.orc.RecordReader = _

  /**
   * Spark Schema.
   */
  private[this] var sparkSchema: StructType = _

  /**
   * Required Schema.
   */
  private[this] var requiredSchema: StructType = _

  /**
   * Partition Column.
   */
  private[this] var partitionColumns: StructType = _

  /**
   * Use index for field lookup.
   */
  private[this] var useIndex: Boolean = false

  /**
   * Use CHAR instead of STRING.
   */
  private[this] var useChar: Boolean = false

  /**
   * Full Schema: requiredSchema + partition schema.
   */
  private[this] var fullSchema: StructType = _

  /**
   * Flags for missing columns.
   */
  private[this] var missingBitSet: BitSet = _

  /**
   * ColumnarBatch for vectorized execution by whole-stage codegen.
   */
  private[this] var columnarBatch: ColumnarBatch = _

  /**
   * The number of rows read and considered to be returned.
   */
  private[this] var rowsReturned: Long = 0L

  /**
   * Total number of rows.
   */
  private[this] var totalRowCount: Long = 0L

  override def getCurrentKey: Void = null

  override def getCurrentValue: ColumnarBatch = columnarBatch

  override def getProgress: Float = rowsReturned.toFloat / totalRowCount

  override def nextKeyValue(): Boolean = nextBatch()

  override def close(): Unit = {
    if (columnarBatch != null) {
      columnarBatch.close()
      columnarBatch = null
    }
    if (rows != null) {
      rows.close()
      rows = null
    }
  }

  override def initialize(inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext): Unit = {}

  // scalastyle:off
  def initialize(
      reader: Reader,
      conf: Configuration,
      start: Long,
      length: Long,
      orcSchema: TypeDescription,
      missingSchema: StructType,
      requiredSchema: StructType,
      partitionColumns: StructType,
      partitionValues: InternalRow,
      useIndex: Boolean,
      useChar: Boolean): Unit = {
  // scalastyle:on
    rows = reader.rows(OrcInputFormat.buildOptions(conf, reader, start, length))
    totalRowCount = reader.getNumberOfRows

    schema = orcSchema
    sparkSchema = CatalystSqlParser.parseDataType(schema.toString).asInstanceOf[StructType]

    batch = schema.createRowBatch(DEFAULT_SIZE)
    logDebug(s"totalRowCount = $totalRowCount")

    this.requiredSchema = requiredSchema
    missingBitSet = new BitSet(requiredSchema.length)
    this.partitionColumns = partitionColumns
    this.useIndex = useIndex
    this.useChar = useChar
    fullSchema = new StructType(requiredSchema.fields ++ partitionColumns.fields)

    columnarBatch = ColumnarBatch.allocate(fullSchema, DEFAULT_MEMORY_MODE, DEFAULT_SIZE)
    if (partitionColumns != null) {
      val partitionIdx = requiredSchema.fields.length
      for (i <- partitionColumns.fields.indices) {
        ColumnVectorUtils.populate(columnarBatch.column(i + partitionIdx), partitionValues, i)
        columnarBatch.column(i + partitionIdx).setIsConstant()
      }
    }
    // Initialize the missing columns once.
    requiredSchema.zipWithIndex.foreach { case (field, index) =>
      if (missingSchema.fieldNames.contains(field.name)) {
        columnarBatch.column(index).putNulls(0, columnarBatch.capacity)
        columnarBatch.column(index).setIsConstant()
        missingBitSet.set(index)
      }
    }
  }

  /**
   * Return true if there exists more data in the next batch. If exists, prepare the next batch
   * by copying from ORC VectorizedRowBatch columns to Spark ColumnarBatch columns.
   */
  private def nextBatch(): Boolean = {
    if (rowsReturned >= totalRowCount) {
      return false
    }

    rows.nextBatch(batch)
    val batchSize = batch.size
    if (batchSize == 0) {
      return false
    }
    rowsReturned += batchSize
    columnarBatch.reset()
    columnarBatch.setNumRows(batchSize)

    var i = 0
    val len = requiredSchema.length
    while (i < len) {
      if (!missingBitSet.get(i)) {
        val field = requiredSchema(i)
        val schemaIndex = if (useIndex) i else schema.getFieldNames.indexOf(field.name)
        assert(schemaIndex >= 0)
        val orcType = schema.getChildren.get(schemaIndex)
        val isPadding = useChar && orcType.getCategory == CHAR
        val maxLen = if (isPadding) orcType.getMaxLength else -1

        val fromColumn = batch.cols(schemaIndex)
        val toColumn = columnarBatch.column(i)

        if (fromColumn.isRepeating) {
          if (fromColumn.isNull(0)) {
            toColumn.appendNulls(batchSize)
          } else {
            field.dataType match {
              case BooleanType =>
                val data = fromColumn.asInstanceOf[LongColumnVector].vector(0) == 1
                toColumn.appendBooleans(batchSize, data)

              case ByteType =>
                val data = fromColumn.asInstanceOf[LongColumnVector].vector(0).toByte
                toColumn.appendBytes(batchSize, data)
              case ShortType =>
                val data = fromColumn.asInstanceOf[LongColumnVector].vector(0).toShort
                toColumn.appendShorts(batchSize, data)
              case IntegerType =>
                val data = fromColumn.asInstanceOf[LongColumnVector].vector(0).toInt
                toColumn.appendInts(batchSize, data)
              case LongType =>
                val data = fromColumn.asInstanceOf[LongColumnVector].vector(0)
                toColumn.appendLongs(batchSize, data)

              case DateType =>
                val data = fromColumn.asInstanceOf[LongColumnVector].vector(0).toInt
                toColumn.appendInts(batchSize, data)

              case TimestampType =>
                val data = fromColumn.asInstanceOf[TimestampColumnVector]
                toColumn.appendLongs(batchSize, data.time(0) * 1000L + data.nanos(0) / 1000L)

              case FloatType =>
                val data = fromColumn.asInstanceOf[DoubleColumnVector].vector(0).toFloat
                toColumn.appendFloats(batchSize, data)
              case DoubleType =>
                val data = fromColumn.asInstanceOf[DoubleColumnVector].vector(0)
                toColumn.appendDoubles(batchSize, data)

              case StringType =>
                val data = fromColumn.asInstanceOf[BytesColumnVector]
                var index = 0
                if (useChar) {
                  while (index < batchSize) {
                    copyString(0, data, toColumn, isPadding, maxLen)
                    index += 1
                  }
                } else {
                  while (index < batchSize) {
                    toColumn.appendByteArray(data.vector(0), data.start(0), data.length(0))
                    index += 1
                  }
                }

              case BinaryType =>
                val data = fromColumn.asInstanceOf[BytesColumnVector]
                var index = 0
                while (index < batchSize) {
                  toColumn.appendByteArray(data.vector(0), data.start(0), data.length(0))
                  index += 1
                }

              case DecimalType.Fixed(precision, scale) =>
                val d = fromColumn.asInstanceOf[DecimalColumnVector].vector(0)
                val value = Decimal(d.getHiveDecimal.bigDecimalValue, d.precision(), d.scale)
                value.changePrecision(precision, scale)
                if (precision <= Decimal.MAX_INT_DIGITS) {
                  toColumn.appendInts(batchSize, value.toUnscaledLong.toInt)
                } else if (precision <= Decimal.MAX_LONG_DIGITS) {
                  toColumn.appendLongs(batchSize, value.toUnscaledLong)
                } else {
                  val bytes = value.toJavaBigDecimal.unscaledValue.toByteArray
                  var index = 0
                  while (index < batchSize) {
                    toColumn.appendByteArray(bytes, 0, bytes.length)
                    index += 1
                  }
                }

              case dt =>
                throw new UnsupportedOperationException(s"Unsupported Data Type: $dt")
            }
          }
        } else if (!field.nullable || fromColumn.noNulls) {
          field.dataType match {
            case BooleanType =>
              val data = fromColumn.asInstanceOf[LongColumnVector].vector
              var index = 0
              while (index < batchSize) {
                toColumn.appendBoolean(data(index) == 1)
                index += 1
              }

            case ByteType =>
              val data = fromColumn.asInstanceOf[LongColumnVector].vector
              var index = 0
              while (index < batchSize) {
                toColumn.appendByte(data(index).toByte)
                index += 1
              }
            case ShortType =>
              val data = fromColumn.asInstanceOf[LongColumnVector].vector
              var index = 0
              while (index < batchSize) {
                toColumn.appendShort(data(index).toShort)
                index += 1
              }
            case IntegerType =>
              val data = fromColumn.asInstanceOf[LongColumnVector].vector
              var index = 0
              while (index < batchSize) {
                toColumn.appendInt(data(index).toInt)
                index += 1
              }
            case LongType =>
              val data = fromColumn.asInstanceOf[LongColumnVector].vector
              toColumn.appendLongs(batchSize, data, 0)

            case DateType =>
              val data = fromColumn.asInstanceOf[LongColumnVector].vector
              var index = 0
              while (index < batchSize) {
                toColumn.appendInt(data(index).toInt)
                index += 1
              }

            case TimestampType =>
              val data = fromColumn.asInstanceOf[TimestampColumnVector]
              var index = 0
              while (index < batchSize) {
                toColumn.appendLong(data.time(index) * 1000L + data.nanos(index) / 1000L)
                index += 1
              }

            case FloatType =>
              val data = fromColumn.asInstanceOf[DoubleColumnVector].vector
              var index = 0
              while (index < batchSize) {
                toColumn.appendFloat(data(index).toFloat)
                index += 1
              }
            case DoubleType =>
              val data = fromColumn.asInstanceOf[DoubleColumnVector].vector
              toColumn.appendDoubles(batchSize, data, 0)

            case StringType =>
              val data = fromColumn.asInstanceOf[BytesColumnVector]
              var index = 0
              if (useChar) {
                while (index < batchSize) {
                  copyString(index, data, toColumn, isPadding, maxLen)
                  index += 1
                }
              } else {
                while (index < batchSize) {
                  toColumn.appendByteArray(
                    data.vector(index), data.start(index), data.length(index))
                  index += 1
                }
              }

            case BinaryType =>
              val data = fromColumn.asInstanceOf[BytesColumnVector]
              var index = 0
              while (index < batchSize) {
                toColumn.appendByteArray(data.vector(index), data.start(index), data.length(index))
                index += 1
              }

            case DecimalType.Fixed(precision, scale) =>
              val data = fromColumn.asInstanceOf[DecimalColumnVector]
              var index = 0
              while (index < batchSize) {
                val d = data.vector(index)
                val value = Decimal(d.getHiveDecimal.bigDecimalValue, d.precision(), d.scale)
                value.changePrecision(precision, scale)
                if (precision <= Decimal.MAX_INT_DIGITS) {
                  toColumn.appendInt(value.toUnscaledLong.toInt)
                } else if (precision <= Decimal.MAX_LONG_DIGITS) {
                  toColumn.appendLong(value.toUnscaledLong)
                } else {
                  val bytes = value.toJavaBigDecimal.unscaledValue.toByteArray
                  toColumn.appendByteArray(bytes, 0, bytes.length)
                }
                index += 1
              }

            case dt =>
              throw new UnsupportedOperationException(s"Unsupported Data Type: $dt")
          }
        } else {
          var index = 0
          while (index < batchSize) {
            if (fromColumn.isNull(index)) {
              toColumn.appendNull()
            } else {
              field.dataType match {
                case BooleanType =>
                  val data = fromColumn.asInstanceOf[LongColumnVector].vector(index) == 1
                  toColumn.appendBoolean(data)
                case ByteType =>
                  val data = fromColumn.asInstanceOf[LongColumnVector].vector(index).toByte
                  toColumn.appendByte(data)
                case ShortType =>
                  val data = fromColumn.asInstanceOf[LongColumnVector].vector(index).toShort
                  toColumn.appendShort(data)
                case IntegerType =>
                  val data = fromColumn.asInstanceOf[LongColumnVector].vector(index).toInt
                  toColumn.appendInt(data)
                case LongType =>
                  val data = fromColumn.asInstanceOf[LongColumnVector].vector(index)
                  toColumn.appendLong(data)

                case DateType =>
                  val data = fromColumn.asInstanceOf[LongColumnVector].vector(index).toInt
                  toColumn.appendInt(data)

                case TimestampType =>
                  val data = fromColumn.asInstanceOf[TimestampColumnVector]
                  toColumn.appendLong(data.time(index) * 1000L + data.nanos(index) / 1000L)

                case FloatType =>
                  val data = fromColumn.asInstanceOf[DoubleColumnVector].vector(index).toFloat
                  toColumn.appendFloat(data)
                case DoubleType =>
                  val data = fromColumn.asInstanceOf[DoubleColumnVector].vector(index)
                  toColumn.appendDouble(data)

                case StringType =>
                  val v = fromColumn.asInstanceOf[BytesColumnVector]
                  if (useChar) {
                    copyString(index, v, toColumn, isPadding, maxLen)
                  } else {
                    toColumn.appendByteArray(v.vector(index), v.start(index), v.length(index))
                  }

                case BinaryType =>
                  val v = fromColumn.asInstanceOf[BytesColumnVector]
                  toColumn.appendByteArray(v.vector(index), v.start(index), v.length(index))

                case DecimalType.Fixed(precision, scale) =>
                  val d = fromColumn.asInstanceOf[DecimalColumnVector].vector(index)
                  val value = Decimal(d.getHiveDecimal.bigDecimalValue, d.precision(), d.scale)
                  value.changePrecision(precision, scale)
                  if (precision <= Decimal.MAX_INT_DIGITS) {
                    toColumn.appendInt(value.toUnscaledLong.toInt)
                  } else if (precision <= Decimal.MAX_LONG_DIGITS) {
                    toColumn.appendLong(value.toUnscaledLong)
                  } else {
                    val bytes = value.toJavaBigDecimal.unscaledValue.toByteArray
                    toColumn.appendByteArray(bytes, 0, bytes.length)
                  }

                case dt =>
                  throw new UnsupportedOperationException(s"Unsupported Data Type: $dt")
              }
            }
            index += 1
          }
        }
      }
      i += 1
    }
    true
  }
}

/**
 * Constants for OrcColumnarBatchReader.
 */
object OrcColumnarBatchReader {
  /**
   * Default memory mode for ColumnarBatch.
   */
  val DEFAULT_MEMORY_MODE = MemoryMode.ON_HEAP

  /**
   * The default size of batch. We use this value for both ORC and Spark consistently
   * because they have different default values like the following.
   *
   * - ORC's VectorizedRowBatch.DEFAULT_SIZE = 1024
   * - Spark's ColumnarBatch.DEFAULT_BATCH_SIZE = 4 * 1024
   */
  val DEFAULT_SIZE: Int = 4 * 1024

  def copyString(
      i: Int,
      data: BytesColumnVector,
      toColumn: org.apache.spark.sql.execution.vectorized.ColumnVector,
      isPadding: Boolean,
      maxLength: Int): Unit = {
    if (isPadding) {
      assert(maxLength > 0)
      val str = UTF8String.fromBytes(data.vector(i), data.start(i), data.length(i))
      val numChars = str.numChars
      if (numChars < maxLength) {
        val padded = UTF8String.concat(str, UTF8String.fromString(" " * (maxLength - numChars)))
        val bytes = padded.getBytes
        toColumn.appendByteArray(bytes, 0, bytes.length)
      } else {
        toColumn.appendByteArray(data.vector(i), data.start(i), data.length(i))
      }
    } else {
      assert(maxLength == -1)
      toColumn.appendByteArray(data.vector(i), data.start(i), data.length(i))
    }
  }
}

