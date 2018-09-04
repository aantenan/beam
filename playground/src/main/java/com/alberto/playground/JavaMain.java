package com.alberto.playground;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class JavaMain {

    public static void main(final String... args) {
        final PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        final Pipeline pipeline = Pipeline.create(options);
        final PCollection<String> input = pipeline.apply(TextIO.read().from("file:///home/alberto/spike/beam/playground/src/main/resources/splitter.data"));
        final PCollection<String> output = input.apply(
                MapElements.into(TypeDescriptors.strings())
                        .via((String word) -> {
                            System.out.println(word);
                            return "Formatted [" + word + "]";
                        }));
    }
}
