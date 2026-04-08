package br.com.bellube.fastchannel.config;

public final class SourceConfig {
    private final int defaultSource;
    private final boolean source2Enabled;
    private final boolean source3Enabled;

    private SourceConfig(int defaultSource, boolean source2Enabled, boolean source3Enabled) {
        this.defaultSource = defaultSource;
        this.source2Enabled = source2Enabled;
        this.source3Enabled = source3Enabled;
    }

    public static SourceConfig from(Integer defaultSource, boolean source2Enabled, boolean source3Enabled) {
        int def = (defaultSource == null ? 1 : defaultSource);
        if (def != 1 && def != 2 && def != 3) def = 1;
        if (def == 2 && !source2Enabled) def = 1;
        if (def == 3 && !source3Enabled) def = 1;
        return new SourceConfig(def, source2Enabled, source3Enabled);
    }

    public int getDefaultSource() { return defaultSource; }
    public boolean isSource1Enabled() { return true; }
    public boolean isSource2Enabled() { return source2Enabled; }
    public boolean isSource3Enabled() { return source3Enabled; }
}
