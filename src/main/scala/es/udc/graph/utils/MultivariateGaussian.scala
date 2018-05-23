package es.udc.graph.utils

import breeze.linalg._
import breeze.numerics.{sqrt, pow, exp}
import breeze.stats.distributions.{ThreadLocalRandomGenerator, RandBasis}
import org.apache.commons.math3.random.MersenneTwister
import scala.math.Pi

/**
 * Multivariate Gaussian
 *
 * Created by David Martinez Rego on 16/11/15.
 */
case class MultivariateGaussian(m: DenseVector[Double], v: DenseMatrix[Double], randSeed: Int) {

  val R : DenseMatrix[Double] = cholesky(v)
  val stdGauss = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(randSeed))).gaussian(0, 1)
  val nConstant = normConstant()
  val iR = invchol(cholesky(v).t)

  /** Draws a new sample from the Multivatiate Gaussian */
  def draw(): DenseVector[Double] = R * DenseVector.rand(m.length, stdGauss) += m

  /** Draws a new matrix of samples from the Multivatiate Gaussian */
  def draw(n : Int): DenseMatrix[Double] = {
    val S : DenseMatrix[Double] = R * DenseMatrix.rand(m.length, n, stdGauss)
    for(i <- 0 to m.length; j <- 0 to n)
      S.update(i, j, S(i,j) + m(i))
    return S
  }

  /**
   * Returns the value of probability density function for a given value of x.
   */
  def pdf(x: Double): Double = pdf(DenseVector(x))

  /**
   * Returns the value of probability density function for a given value of vector x.
   */
  def pdf(x: DenseVector[Double]): Double = nConstant * exp(-0.5 * ((x - m).t * (iR * (x - m))))


  def invchol(R:DenseMatrix[Double]): DenseMatrix[Double] = {
    val Rinv = inv(R)
    Rinv*Rinv.t
  }

  def normConstant(): Double = 1d / (pow(2 * Pi, m.size.toDouble / 2) * sqrt(det(v)))
}

object MultivariateGaussian {

  /**
   * Creates a Gaussian sampler for scalars
   *
   * @param m Mean
   * @param v Variance
   */
  def apply(m: Double, v: Double, randSeed: Int): MultivariateGaussian =
      new MultivariateGaussian(DenseVector(m), DenseMatrix(v), randSeed)

  /**
   * Creates a Standard Multivariate Gaussian sampler for a specific dimension
   *
   * @param dim dimension
   * @param randSeed seed
   */
  def apply(dim : Int, randSeed: Int): MultivariateGaussian =
    new MultivariateGaussian(DenseVector.zeros(dim), DenseMatrix.eye(dim), randSeed)
}