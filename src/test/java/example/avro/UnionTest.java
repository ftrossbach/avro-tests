package example.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

class UnionTest {

    @Test
    public void saveAsString() throws IOException {
        var union = Union.newBuilder()
                .setSomeUnionField("I am a String")
                .build();


        assertThat(union.getSomeUnionField()).isInstanceOf(CharSequence.class);

        File file = serialize(union);

        var userDatumReader = new SpecificDatumReader<>(Union.class);
        var dataFileReader = new DataFileReader<>(file, userDatumReader);


        assertThat(dataFileReader.hasNext()).isTrue();

        while(dataFileReader.hasNext()){
            var deserializedUnion = dataFileReader.next();
            assertThat(deserializedUnion.getSomeUnionField()).isInstanceOf(CharSequence.class);
        }

    }

    @Test
    public void saveAsInteger() throws IOException {
        var union = Union.newBuilder()
                .setSomeUnionField(42)
                .build();

        assertThat(union.getSomeUnionField()).isInstanceOf(Integer.class);

        File file = serialize(union);

        var userDatumReader = new SpecificDatumReader<>(Union.class);
        var dataFileReader = new DataFileReader<>(file, userDatumReader);


        assertThat(dataFileReader.hasNext()).isTrue();

        while(dataFileReader.hasNext()){
            var deserializedUnion = dataFileReader.next();
            assertThat(deserializedUnion.getSomeUnionField()).isInstanceOf(Integer.class);
        }

    }

    @Test
    public void saveAsDouble() throws IOException {
        var union = Union.newBuilder()
                .setSomeUnionField(3.14)
                .build();

        assertThat(union.getSomeUnionField()).isInstanceOf(Double.class);

        File file = serialize(union);

        var userDatumReader = new SpecificDatumReader<>(Union.class);
        var dataFileReader = new DataFileReader<>(file, userDatumReader);


        assertThat(dataFileReader.hasNext()).isTrue();

        while(dataFileReader.hasNext()){
            var deserializedUnion = dataFileReader.next();
            assertThat(deserializedUnion.getSomeUnionField()).isInstanceOf(Double.class);
        }

    }

    private File serialize(Union union) throws IOException {
        var writer = new SpecificDatumWriter<>(Union.class);
        try(var fileWriter = new DataFileWriter<>(writer)){
            var file = File.createTempFile("test", ".avro");
            fileWriter.create(union.getSchema(), file);
            fileWriter.append(union);
            return file;
        }

    }
}