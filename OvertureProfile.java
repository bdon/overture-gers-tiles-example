import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.util.Glob;
import com.onthegomap.planetiler.reader.parquet.ParquetFeature;
import org.apache.parquet.schema.MessageType;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

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
        gersToParcelnumb = loadBridge(Path.of("dallas_bridge.csv"));
        parcelnumbToZoning = loadParcels(Path.of("tx_dallas.csv"));
    }

    private static Map<String, String> loadBridge(Path path) {
        Map<String, String> map = new HashMap<>();
        var mapper = new CsvMapper();
        var schema = CsvSchema.emptySchema().withHeader();
        try (var it = mapper.readerFor(Map.class).with(schema).readValues(path.toFile())) {
            while (it.hasNext()) {
                @SuppressWarnings("unchecked")
                Map<String, String> row = (Map<String, String>) it.next();
                map.put(row.get("gers_id"), row.get("ll_uuid"));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return map;
    }

    private static Map<String, String[]> loadParcels(Path path) {
        Map<String, String[]> map = new HashMap<>();
        var mapper = new CsvMapper();
        var schema = CsvSchema.emptySchema().withHeader();
        try (var it = mapper.readerFor(Map.class).with(schema).readValues(path.toFile())) {
            while (it.hasNext()) {
                @SuppressWarnings("unchecked")
                Map<String, String> row = (Map<String, String>) it.next();
                String zoningType = row.getOrDefault("zoning_type", "");
                String zoningSubtype = row.getOrDefault("zoning_subtype", "");
                if (!zoningType.isEmpty() || !zoningSubtype.isEmpty()) {
                    map.put(row.get("ll_uuid"), new String[]{zoningType, zoningSubtype});
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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

        String id = source.getString("id");
        if (id != null) {
            String llUuid = gersToParcelnumb.get(id);
            if (llUuid != null) {
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