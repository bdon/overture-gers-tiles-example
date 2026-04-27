import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.util.Glob;
import com.onthegomap.planetiler.reader.parquet.ParquetFeature;
import org.apache.parquet.schema.MessageType;

import java.nio.file.Path;
import java.util.List;

public class OvertureProfile implements Profile {

    public interface Theme {
        void processFeature(SourceFeature source, FeatureCollector features);
    }

    public OvertureProfile() {

    }

    protected static void addFullTags(SourceFeature source, FeatureCollector.Feature feature, int minZoomToShowAlways) {
        if (source instanceof ParquetFeature pf) {
            MessageType schema = pf.parquetSchema();
            for (var field : schema.getFields()) {
                var name = field.getName();
                if (!pf.hasTag(name)) continue;
                if (name.equals("bbox") || name.equals("geometry")) continue;
                if (name.equals("names")) {
                    var primaryName = pf.getStruct("names").get("primary");
                    feature.setAttrWithMinSize("@name", primaryName, 16, 0, minZoomToShowAlways);
                }
                if (field.isPrimitive()) {
                    feature.inheritAttrFromSource(name);
                    feature.setAttrWithMinSize(name, source.getTag(name), 16, 0, minZoomToShowAlways);
                } else {
                    feature.setAttrWithMinSize(name, source.getStruct(name).asJson(), 16, 0, minZoomToShowAlways);
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