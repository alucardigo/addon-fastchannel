package br.com.bellube.fastchannel.job;

import br.com.bellube.fastchannel.service.DeparaService;
import br.com.bellube.fastchannel.service.QueueService;
import br.com.bellube.fastchannel.util.DBUtil;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Job de varredura periodica de alteracoes pontuais que NAO disparam listener
 * confiavelmente para enfileirar PRECO/UPDATE no AD_FCQUEUE.
 *
 * Cobre dois cenarios reportados em prod 2026-06-01:
 * <ol>
 *   <li><b>Escalonado expirado nao limpo</b> — quando o TGFDES.DTFINAL passa, nenhum
 *       evento dispara para os produtos afetados; os batches obsoletos ficam visiveis
 *       na FC ate o proximo full sync (horas/dia). Aqui detectamos promos que
 *       acabaram de expirar e enfileiramos uma sync que ja faz o cleanup.</li>
 *   <li><b>Alteracao manual em TGFEXC (Excecao de Preco) que o PrecoListener nao
 *       capturou</b> — o listener depende do nome de instancia JAPE; quando a edicao
 *       vem por um caminho fora do JAPE (ex.: importacao em lote, script MGE),
 *       a fila fica sem entrada. Aqui varremos TGFEXC.DHALTREG desde a ultima
 *       varredura e enfileiramos qualquer produto afetado.</li>
 * </ol>
 *
 * Idempotente: {@code QueueService.enqueue} ja aplica debounce, entao reexecucoes
 * seguidas nao geram filas duplicadas. O outbox processor pega cada item enfileirado
 * e roda {@code PriceService.syncPrice} (com o override v1.2.87 + dedup/cleanup v1.2.85).
 */
public class AutoPriceChangesSweepJob {

    private static final Logger log = Logger.getLogger(AutoPriceChangesSweepJob.class.getName());

    /**
     * Cursor "ultima varredura concluida" para TGFEXC. In-memory: no restart volta para "1 hora atras"
     * para nao perder alteracoes durante a janela de boot. Promos expiradas usam janela propria
     * (ver SQL_RECENTLY_EXPIRED_PROMOS / expiredWindowDays), independente deste cursor.
     */
    private static final AtomicReference<Instant> LAST_SWEEP =
            new AtomicReference<>(Instant.now().minusSeconds(3600));

    /**
     * [HARDEN 2026-06-15 v1.2.91] Janela de deteccao de promos expiradas.
     *
     * Antes era fixa em 2 dias — fragil: se o addon ficasse fora do ar (ou o full sync, que
     * leva ~4h, fosse interrompido por restart) por mais de 2 dias apos a expiracao, o escalonado
     * nunca era limpo e ficava "ativo" na FC indefinidamente (incidente 2026-06-15: promo 418 do
     * produto 9102 expirou 10/06 e so foi limpa por um full sync em 15/06). Agora a janela e
     * generosa (30 dias, configuravel via -Dfc.auto.sweep.expiredDays) e cada promo e enfileirada
     * UMA unica vez por vida do addon (HANDLED_EXPIRED_PROMOS) — sem re-enfileirar a cada 10min.
     * No restart o set zera e re-varre os ultimos N dias (one-shot idempotente).
     */
    private static final int DEFAULT_EXPIRED_WINDOW_DAYS = 30;

    private static int expiredWindowDays() {
        try {
            String p = System.getProperty("fc.auto.sweep.expiredDays");
            if (p != null && !p.trim().isEmpty()) {
                return Math.max(2, Math.min(120, Integer.parseInt(p.trim())));
            }
        } catch (Exception ignore) {
            // valor invalido -> default
        }
        return DEFAULT_EXPIRED_WINDOW_DAYS;
    }

    /** Promos (DTFINAL no passado, dentro da janela) com faixas, versao mais recente. */
    private static final String SQL_RECENTLY_EXPIRED_PROMOS =
            "SELECT DISTINCT D.CODPROD, D.NUPROMOCAO FROM TGFDES D " +
            "WHERE D.DTFINAL >= CAST(DATEADD(day, ?, GETDATE()) AS DATE) " +
            "  AND D.DTFINAL <  CAST(GETDATE() AS DATE) " +
            "  AND EXISTS (SELECT 1 FROM TGFDPQ Q WHERE Q.NUPROMOCAO = D.NUPROMOCAO) " +
            "  AND ISNULL(D.NUVERSAO,0) = (SELECT MAX(ISNULL(S.NUVERSAO,0)) FROM TGFDES S WHERE S.NUPROMOCAO = D.NUPROMOCAO)";

    /**
     * Promos (NUPROMOCAO) cujo cleanup ja foi enfileirado nesta vida do addon.
     * Evita re-enfileirar a mesma promo expirada a cada ciclo de 10min (o cleanup e idempotente,
     * mas re-enfileirar e desperdicio). Reseta no restart -> re-varre a janela (one-shot).
     */
    private static final Set<BigDecimal> HANDLED_EXPIRED_PROMOS =
            java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());

    /** Excecoes de preco (TGFEXC) alteradas desde a ultima varredura. */
    private static final String SQL_TGFEXC_CHANGED_SINCE =
            "SELECT DISTINCT CODPROD FROM TGFEXC WHERE DHALTREG > ?";

    public void run() {
        Instant since = LAST_SWEEP.get();
        Instant now = Instant.now();

        Set<BigDecimal> products = new LinkedHashSet<>();
        int expiredCount = collectRecentlyExpiredPromos(products);
        int tgfexcCount = collectTgfexcChangedSince(products, Timestamp.from(since));

        if (products.isEmpty()) {
            log.info("[AutoSweep] varredura ok: nada a fazer (expired=" + expiredCount
                    + " tgfexc=" + tgfexcCount + ", desde " + since + ")");
            LAST_SWEEP.set(now);
            return;
        }

        QueueService queue = QueueService.getInstance();
        DeparaService depara = DeparaService.getInstance();
        int enqueued = 0;
        int skippedNoSku = 0;
        int skippedError = 0;
        for (BigDecimal codProd : products) {
            try {
                String sku = depara.getSkuForStock(codProd);
                if (sku == null) {
                    skippedNoSku++;
                    continue;
                }
                sku = sku.trim();
                if (sku.isEmpty()) {
                    skippedNoSku++;
                    continue;
                }
                queue.enqueuePrice(codProd, sku);
                enqueued++;
            } catch (Exception e) {
                skippedError++;
                log.log(Level.FINE, "[AutoSweep] falha enfileirando CODPROD=" + codProd, e);
            }
        }
        log.info("[AutoSweep] varredura ok: expired=" + expiredCount + " tgfexc=" + tgfexcCount
                + " enfileirados=" + enqueued + " sem_sku=" + skippedNoSku + " erro=" + skippedError
                + " (debounce QueueService cuida de duplicatas)");
        LAST_SWEEP.set(now);
    }

    private int collectRecentlyExpiredPromos(Set<BigDecimal> out) {
        int count = 0;
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            ps = conn.prepareStatement(SQL_RECENTLY_EXPIRED_PROMOS);
            ps.setInt(1, -Math.abs(expiredWindowDays())); // janela: N dias atras
            rs = ps.executeQuery();
            while (rs.next()) {
                BigDecimal codProd = rs.getBigDecimal(1);
                BigDecimal nuPromo = rs.getBigDecimal(2);
                if (codProd == null) {
                    continue;
                }
                // Dedup: cada promo expirada gera UM cleanup por vida do addon (cleanup e idempotente).
                if (nuPromo != null && !HANDLED_EXPIRED_PROMOS.add(nuPromo)) {
                    continue; // ja enfileirada nesta sessao
                }
                out.add(codProd);
                count++;
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "[AutoSweep] erro buscando promos expiradas", e);
        } finally {
            DBUtil.closeAll(rs, ps, conn);
        }
        return count;
    }

    private int collectTgfexcChangedSince(Set<BigDecimal> out, Timestamp since) {
        int count = 0;
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection();
            ps = conn.prepareStatement(SQL_TGFEXC_CHANGED_SINCE);
            ps.setTimestamp(1, since);
            rs = ps.executeQuery();
            while (rs.next()) {
                BigDecimal codProd = rs.getBigDecimal(1);
                if (codProd != null) {
                    out.add(codProd);
                    count++;
                }
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "[AutoSweep] erro buscando TGFEXC alterados", e);
        } finally {
            DBUtil.closeAll(rs, ps, conn);
        }
        return count;
    }
}
