import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.util.Glob;
import com.onthegomap.planetiler.reader.parquet.ParquetFeature;
import org.apache.parquet.schema.MessageType;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.LocalInputFile;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OvertureProfile implements Profile {

    public interface Theme {
        void processFeature(SourceFeature source, FeatureCollector features);
    }

    private final Map<String, String> gersToParcelnumb;
    private final Map<String, String[]> parcelnumbToZoning;

    public OvertureProfile() {
        gersToParcelnumb = loadBridge(Path.of("dallas_bridge.parquet"));
        parcelnumbToZoning = loadParcels(Path.of("tx_dallas.parquet"));
    }

    private static String getStringOrEmpty(Group group, String field) {
        return group.getFieldRepetitionCount(field) > 0 ? group.getString(field, 0) : "";
    }

    private static void readParquet(Path path, java.util.function.Consumer<Group> consumer) {
        try (var fileReader = ParquetFileReader.open(new LocalInputFile(path))) {
            var schema = fileReader.getFooter().getFileMetaData().getSchema();
            var columnIO = new ColumnIOFactory().getColumnIO(schema);
            org.apache.parquet.column.page.PageReadStore pages;
            while ((pages = fileReader.readNextRowGroup()) != null) {
                var recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                for (long i = 0; i < pages.getRowCount(); i++) {
                    consumer.accept(recordReader.read());
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Map<String, String> loadBridge(Path path) {
        Map<String, String> map = new HashMap<>();
        readParquet(path, row -> map.put(row.getString("gers_id", 0), row.getString("ll_uuid", 0)));
        return map;
    }

    private static Map<String, String[]> loadParcels(Path path) {
        Map<String, String[]> map = new HashMap<>();
        readParquet(path, row -> {
            String zoningType = getStringOrEmpty(row, "zoning_type");
            String zoningSubtype = getStringOrEmpty(row, "zoning_subtype");
            if (!zoningType.isEmpty() || !zoningSubtype.isEmpty()) {
                map.put(row.getString("ll_uuid", 0), new String[]{zoningType, zoningSubtype});
            }
        });
        return map;
    }

    protected static void addFullTags(SourceFeature source, FeatureCollector.Feature feature, int minZoomToShowAlways) {
        if (source instanceof ParquetFeature pf) {
            MessageType schema = pf.parquetSchema();
            for (var field : schema.getFields()) {
                var name = field.getName();
                if (name.equals("names")) {
                    var primaryName = pf.getStruct("names").get("primary");
                    feature.setAttrWithMinSize("@name", primaryName, 16, 0, minZoomToShowAlways);
                }
            }
        }
    }

    protected static FeatureCollector.Feature createAnyFeature(SourceFeature feature,
                                                               FeatureCollector features) {
        return feature.isPoint() ? features.point(feature.getSourceLayer()) :
                feature.canBePolygon() ? features.polygon(feature.getSourceLayer()) :
                        features.line(feature.getSourceLayer());
    }

    @Override
    public void processFeature(SourceFeature source, FeatureCollector features) {
        String layer = source.getSourceLayer();
        var point = features.point(layer).setMinZoom(10);
        OvertureProfile.addFullTags(source, point, 10);

        var confidence = source.getTag("confidence");
        if (confidence != null) {
            point.setAttr("confidence", confidence);
        }

        String id = source.getString("id");
        if (id != null) {
            point.setAttr("gers_id", id);
            String llUuid = gersToParcelnumb.get(id);
            if (llUuid != null) {
                point.setAttr("ll_uuid", llUuid);
                String[] zoning = parcelnumbToZoning.get(llUuid);
                if (zoning != null) {
                    point.setAttr("zoning_type", zoning[0]);
                    point.setAttr("zoning_subtype", zoning[1]);
                }
            }
        }
    }

    @Override
    public boolean isOverlay() {
        return true;
    }

    @Override
    public String name() {
        return "Overture Places + Regrid";
    }

    @Override
    public String description() {
        return "A tileset generated from Overture data";
    }

    @Override
    public String attribution() {
        return """
                <a href="https://www.openstreetmap.org/copyright" target="_blank">&copy; OpenStreetMap</a>
                <a href="https://docs.overturemaps.org/attribution" target="_blank">&copy; Overture Maps Foundation</a>
                """
                .replace("\n", " ")
                .trim();
    }

    public static void main(String[] args) throws Exception {
        OvertureProfile.run(Arguments.fromArgsOrConfigFile(args));
    }

    static void run(Arguments args) throws Exception {
        Planetiler.create(args)
                .setProfile(new OvertureProfile())
                .addParquetSource("overture",
                        List.of(Path.of("overture_dallas.parquet")))
                .overwriteOutput(Path.of("dallas.pmtiles"))
                .run();
    }
}