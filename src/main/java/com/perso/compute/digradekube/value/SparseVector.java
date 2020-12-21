package com.perso.compute.digradekube.value;


// importing generic packages
import io.grpc.stub.StreamObserver;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class SparseVector {

    // TreeMap is used to maintain sorted order
    private TreeMap<Integer, Double> st;
    private int size;

    // Constructor
    public SparseVector(int size) {
        this.size = size;
        this.st = new TreeMap<Integer, Double>();
    }

    public SparseVector(String stringVector) {
        String[] elements = stringVector.split("@");
        this.size = Integer.parseInt(elements[0]);
        HashMap<Integer, Double> map = (HashMap<Integer, Double>)
                Arrays.asList(elements[1].substring(1, elements[1].length()-1).split(","))
                .stream().map(s -> s.split("=")).collect(Collectors.toMap(e -> Integer.parseInt(e[0].trim()), e -> Double.parseDouble(e[1].trim())));
        this.st = new TreeMap<Integer, Double>(map);
    }

    /**
     * Populate with random double values.
     */
    public void populate() {
        Random rand = new Random();
        for (int i = 0; i < this.size; i++) {
            this.st.put(rand.nextInt(this.size), ThreadLocalRandom.current().nextDouble(0, 1));
        }
    }

    // Function to insert a (index, value) pair
    public void put(int i, double value)
    {
        // checking if index(i) is out of bounds
        if (i < 0 || i >= size) {
            throw new RuntimeException("\nError : Out of Bounds\n");
        }
        if (value == 0.0) {
            st.remove(i);
        }
        else {
            st.put(i, value);
        }
    }

    // Function to get value for an index
    public double get(int i) {
        if (i < 0 || i >= size)
            throw new RuntimeException("\nError : Out of Bounds\n");
        if (st.containsKey(i))
            return st.get(i);
        else
            return 0.0;
    }

    // Function to get size of the vector
    public int size() { return size; }

    // Function to get dot product of two vectors
    public double dot(SparseVector b) {
        SparseVector a = this;
        if (a.size != b.size) {
            throw new RuntimeException("Error : Vector lengths are not same");
        }
        double sum = 0.0;
        if (a.st.size() <= b.st.size()) {
            for (Map.Entry<Integer, Double> entry : a.st.entrySet()) {
                if (b.st.containsKey(entry.getKey())) {
                    sum += a.get(entry.getKey()) * b.get(entry.getKey());
                }
            }
        } else {
            for (Map.Entry<Integer, Double> entry : b.st.entrySet()) {
                if (a.st.containsKey(entry.getKey())) {
                    sum += a.get(entry.getKey()) * b.get(entry.getKey());
                }
            }
        }
        return sum;
    }

    public SparseVector plus(SparseVector b) {
        SparseVector a = this;
        if (a.size != b.size) {
            throw new RuntimeException("Error : Vector lengths are not same");
        }
        SparseVector c = new SparseVector(size);
        for (Map.Entry<Integer, Double> entry : a.st.entrySet()) {
            c.put(entry.getKey(), a.get(entry.getKey()));
        }
        for (Map.Entry<Integer, Double> entry : b.st.entrySet()) {
            c.put(entry.getKey(), b.get(entry.getKey()) + c.get(entry.getKey()));
        }
        return c;
    }

    // Function toString() for printing vector
    public String toString() {
        return this.size + "@" + st.toString();
    }

    /**
     * Compute norm-1 of a vector.
     */
    public double norm1() {
        double norm1 = 0.0;
        for (Map.Entry<Integer, Double> entry : st.entrySet()) {
            norm1 = Math.max(Math.abs(entry.getValue()), norm1);
        }
        return norm1;
    }
}
