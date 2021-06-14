/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.mapreduceassignment;

/**
 *
 * @author stephen
 */
public class Testing {
    public static void main(String[] args) {
        for(int i=1;i<10000;i+=1000) {
            for(int j=1;j<1000;j+=100) {
                System.out.println("Mapping Lines per thread: " + i + " Reducing words per thread: " + j);
                String[] arguments = new String[]{"file1.txt", "file2.txt", "file3.txt", Integer.toString(i), Integer.toString(j)};
                MapReduceFiles.main(arguments);
            }
        }
    }
}
