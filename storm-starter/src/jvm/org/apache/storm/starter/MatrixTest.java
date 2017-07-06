package org.apache.storm.starter;


import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.LUDecomposition;


/**
 * Created by VagelisAkis on 3/1/2017.
 */
public class MatrixTest {

    public static Array2DRowRealMatrix createMatrix(int n) {
        double[][] M = new double[n][n];

        // Odd order

        if ((n % 2) == 1) {
            int a = (n+1)/2;
            int b = (n+1);
            for (int j = 0; j < n; j++) {
                for (int i = 0; i < n; i++) {
                    M[i][j] = n*((i+j+a) % n) + ((i+2*j+b) % n) + 1;
                }
            }
        // Doubly Even Order
        }
        else if ((n % 4) == 0) {
            for (int j = 0; j < n; j++) {
                for (int i = 0; i < n; i++) {
                    if (((i+1)/2)%2 == ((j+1)/2)%2) {
                        M[i][j] = n*n-n*i-j;
                    } else {
                        M[i][j] = n*i+j+1;
                    }
                }
            }
        // Singly Even Order
        } else {
            int p = n / 2;
            int k = (n - 2) / 4;
            Array2DRowRealMatrix A = createMatrix(p);
            for (int j = 0; j < p; j++) {
                for (int i = 0; i < p; i++) {
                    double aij = A.getEntry(i, j);
                    M[i][j] = aij;
                    M[i][j + p] = aij + 2 * p * p;
                    M[i + p][j] = aij + 3 * p * p;
                    M[i + p][j + p] = aij + p * p;
                }
            }
            for (int i = 0; i < p; i++) {
                for (int j = 0; j < k; j++) {
                    double t = M[i][j]; M[i][j] = M[i+p][j]; M[i+p][j] = t;
                }
                for (int j = n-k+1; j < n; j++) {
                    double t = M[i][j]; M[i][j] = M[i+p][j]; M[i+p][j] = t;
                }
            }
            double t = M[k][0]; M[k][0] = M[k+p][0]; M[k+p][0] = t;
            t = M[k][k]; M[k][k] = M[k+p][k]; M[k+p][k] = t;
        }

        return new Array2DRowRealMatrix(M);
    }

    public static void printMatrix(Array2DRowRealMatrix matrix) {
        double[][] array = matrix.getData();

        for(int i = 0; i < array.length; i++) {
            for (int j = 0; j < array[i].length; j++)
                System.out.print(" " + array[i][j]);
            System.out.print("\n");
        }
    }

    public static void main(String args[]) {

        double start = System.nanoTime();

        double[][] A={{4.00,3.00},{2.00,1.00}};
//        double[][] B={{-0.500,1.500},{1.000,-2.0000}};
        double[][] I={{1,0},{0,1}};

        Array2DRowRealMatrix a = new Array2DRowRealMatrix(A);
//        Array2DRowRealMatrix b = new Array2DRowRealMatrix(B);
        Array2DRowRealMatrix i = new Array2DRowRealMatrix(I);

        LUDecomposition lu = new LUDecomposition(a);

        Array2DRowRealMatrix inv = (Array2DRowRealMatrix) lu.getSolver().solve(i);

        double stop = System.nanoTime();

        System.out.println(stop-start);

        printMatrix(inv);

    }
}
