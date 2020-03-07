package com.example.simmilarweb.common;

import io.vavr.Tuple;
import io.vavr.Tuple2;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @param <T>
 *     help composite components in a pipeline like order
 *     similar to  streams api(java 8+) fashion without it's complexities
 */
public class PipelineCommon<T> {

    private T value;

    private static final PipelineCommon<?> EMPTY = new PipelineCommon<>();

    private PipelineCommon() {
        this.value = null;
    }

    private PipelineCommon(T initial) {
        value = initial;
    }

    private boolean isPresent() {
        return value != null;
    }

    public T getValue() {
        return value;
    }
    public static <T> PipelineCommon<T> of(T initial){
        return new PipelineCommon<>(initial);
    }



    public <R> PipelineCommon<R> let(Function<T,R> mapper){
        return new PipelineCommon<>(mapper.apply(value));
    }

    public <R> PipelineCommon<T> letLast(Function<T,R> mapper){
        R res = mapper.apply(value);
        return new PipelineCommon<>(value);
    }

    public <R> PipelineCommon<R> onError(Function<T,R> mapper){

        return new PipelineCommon<>(mapper.apply(value));
    }


    public <R> R letAndReturn(Function<T,R> mapper){
        return let(mapper).getValue();
    }


    public <R,S> PipelineCommon<Tuple2<R,S>> branch(Function<T,R> mapper1, Function<T,S> mapper2) {

        R apply = mapper1.apply(value);
        S apply1 = mapper2.apply(value);
        Tuple2<R, S> tuple = Tuple.of(apply, apply1);
        return new PipelineCommon<>(tuple);

    }


    public PipelineCommon<T> filter(Predicate<? super T> predicate) {
        Objects.requireNonNull(predicate);
        return predicate.test(value) ? this : empty();
    }

    public <T> PipelineCommon<T> empty() {
        @SuppressWarnings("unchecked")
        PipelineCommon<T> t = (PipelineCommon<T>) EMPTY;
        return t;
    }

    public PipelineCommon<Boolean> test(Predicate<T> predicate){

        return new PipelineCommon<>(predicate.test(value));

    }

    public <R> PipelineCommon<R> map(Function<T,R> mapper){
        return let(mapper);
    }

    public  <R> PipelineCommon<R> flatMap(Function<T, PipelineCommon<R>> mapper) {
        return mapper.apply(value);
    }
}
