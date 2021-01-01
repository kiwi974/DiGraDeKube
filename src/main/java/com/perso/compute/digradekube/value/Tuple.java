package com.perso.compute.digradekube.value;

public class Tuple<T1, T2> {

    private final T1 t1;
    private final T2 t2;

    public Tuple() {
        this.t1 = null;
        this.t2 = null;
    }

    private Tuple(T1 t1, T2 t2) {
        this.t1 = t1;
        this.t2 = t2;
    }

    public Tuple of(T1 t1, T2 t2) {
        return new Tuple(t1, t2);
    }

    public T1 getT1() { return t1; }

    public T2 getT2() { return t2; }

    public boolean equals(Tuple tuple) {
        return (tuple.getT1().equals(this.t1)) && (tuple.getT2().equals(this.t2));
    }
}
