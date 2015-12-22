package sparkExamples.LinearAlgebra


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import breeze.linalg._


/**
  * Created by rcoleman on 12/21/15.
  */
object MatrixMultiplication {
  def main(args: Array[String]): Unit = {

    val appName = "matrix multiplication test"
    val conf = new SparkConf().setAppName(appName).setMaster("local[16]")
    val sc = new SparkContext(conf)

    val rowsM = 20
    val colsM = 34
    val rowsN = colsM
    val colsN = 10


    val M: DenseMatrix[Double] = DenseMatrix.rand(rowsM, colsM)
    val N: DenseMatrix[Double] = DenseMatrix.rand(rowsN, colsN)
    val distM = DistributedMatrix(sc, M)
    val distN = DistributedMatrix(sc, N)

    println("left Matrix: ")
    println(M)
    println("right Matrix: ")
    println(N)

    println("multiplied breeze: ")
    val MN: DenseMatrix[Double] = M * N
    println(MN)

    val distMN = distM * distN
    val localDMN = distMN.toDenseMatrix
    println("multiplied dist: ")
    println(localDMN)
    val diff: Double = sum(MN - localDMN)
    println(s"diff: $diff")

  }
}

object DistributedMatrix {
  def apply(matrix: RDD[DistributethisatrixElement]): DistributedMatrix = new DistributedMatrix(matrix)
  def apply(sc: SparkContext, matrix: DenseMatrix[Double]): DistributedMatrix = {
    val rows = matrix.rows
    val cols = matrix.cols
    val elements: IndexedSeq[DistributethisatrixElement] = (0 until rows).flatMap{ r =>
      (0 until cols).map{ c =>
        DistributethisatrixElement(row = r, column = c, value = matrix(r,c))
      }
    }
    new DistributedMatrix(sc.parallelize(elements))
  }
}

class DistributedMatrix(val matrix: RDD[DistributethisatrixElement]) extends Serializable {

  lazy val cols: Long = {
    this.matrix.map{ _.column }.max() + 1
  }
  lazy val rows: Long = {
    this.matrix.map{ _.row }.max() + 1
  }

  def *(other: DistributedMatrix): DistributedMatrix = {
    val rows = this.rows
    val cols = other.cols
    val thisAgg = this.matrix.flatMap(this.matrixMultiplicationMapLeft(cols))
    val otherAgg = other.matrix.flatMap(other.matrixMultiplicationMapRight(rows))
    val aggregators: RDD[((Long, Long), MatrixMultiplicationAggregator)] = thisAgg ++ otherAgg
    val newMatrixRDD: RDD[DistributethisatrixElement] = aggregators.reduceByKey(this.matrixMultiplicationReduceByKey)
      .map(this.matrixMultiplicationMapBackToMatrix)
    DistributedMatrix(newMatrixRDD)
  }

  def toDenseMatrix: DenseMatrix[Double] = {
    val rows = this.rows
    val cols = this.cols
    val orderedValues: Array[Double] = (0L until cols).flatMap{ c =>
      this.matrix.filter( _.column == c).collect().sortBy( _.row ).map{ _.value }
    }.toArray

    new DenseMatrix(rows.toInt, cols.toInt, orderedValues)
  }

  def matrixMultiplicationMapLeft(colsOfOther: Long)(matrixElement: DistributethisatrixElement): Seq[((Long,Long),MatrixMultiplicationAggregator)] = {
    (0L until colsOfOther).map{ k =>
      val newMatrixIndex = (matrixElement.row, k)
      val aggregatorMap = MatrixMultiplicationAggregator(aggMap = Map(matrixElement.column -> matrixElement.value))
      (newMatrixIndex, aggregatorMap)
    }
  }

  def matrixMultiplicationMapRight(rowsOfOther: Long)(matrixElement: DistributethisatrixElement): Seq[((Long,Long),MatrixMultiplicationAggregator)] = {
    (0L until rowsOfOther).map { k =>
      val newMatrixIndex = (k, matrixElement.column)
      val aggregatorMap = MatrixMultiplicationAggregator(aggMap = Map(matrixElement.row -> matrixElement.value))
      (newMatrixIndex, aggregatorMap)
    }
  }

  def matrixMultiplicationReduceByKey(left: MatrixMultiplicationAggregator, right: MatrixMultiplicationAggregator) = {
    val intersectKeys: Set[Long] = left.aggMap.keySet.intersect(right.aggMap.keySet)
    val summed: Double = left.summed + right.summed +
      intersectKeys.map{ j => left.aggMap(j) * right.aggMap(j)}.sum

    val leftDiffKeys: Set[Long] = left.aggMap.keySet.diff(right.aggMap.keySet)
    val leftDiff: Map[Long, Double] = leftDiffKeys.map{ j => j -> left.aggMap(j)}.toMap
    val rightDiffKeys: Set[Long] = right.aggMap.keySet.diff(left.aggMap.keySet)
    val rightDiff: Map[Long, Double] = rightDiffKeys.map{ j => j -> right.aggMap(j)}.toMap

    MatrixMultiplicationAggregator(summed = summed, aggMap = leftDiff ++ rightDiff)
  }

  def matrixMultiplicationMapBackToMatrix(input: ((Long,Long), MatrixMultiplicationAggregator)): DistributethisatrixElement = {
    DistributethisatrixElement(row = input._1._1, column = input._1._2, value = input._2.summed)
  }
}

case class DistributethisatrixElement(row: Long, column: Long, value: Double) extends Serializable
case class MatrixMultiplicationAggregator(summed: Double = 0.0, aggMap: Map[Long,Double]) extends Serializable