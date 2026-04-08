package br.com.bellube.fastchannel.service;

import br.com.bellube.fastchannel.dto.OrderCustomerDTO;
import br.com.bellube.fastchannel.dto.OrderDTO;
import br.com.bellube.fastchannel.dto.OrderPaymentDetailsDTO;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FastchannelHeaderMappingServiceTest {

    @Test
    public void resolvesTipNegFromPaymentMethodBeforeConfig() throws Exception {
        OrderDTO order = new OrderDTO();
        order.setOrderId("1");
        order.setCustomer(new OrderCustomerDTO());

        OrderPaymentDetailsDTO payment = new OrderPaymentDetailsDTO();
        payment.setPaymentMethodId(6);
        order.setCurrentPaymentDetails(payment);

        FastchannelHeaderMappingService.ConfigLookup config = new FastchannelHeaderMappingService.ConfigLookup() {
            @Override
            public BigDecimal getCodemp() {
                return BigDecimal.ONE;
            }

            @Override
            public BigDecimal getCodTipOper() {
                return new BigDecimal(2);
            }

            @Override
            public BigDecimal getTipNeg() {
                return new BigDecimal(3);
            }

            @Override
            public BigDecimal getCodNat() {
                return null;
            }

            @Override
            public BigDecimal getCodCenCus() {
                return null;
            }

            @Override
            public BigDecimal getCodVendPadrao() {
                return null;
            }
        };

        FastchannelHeaderMappingService.TipNegLookup tipNegLookup = new FastchannelHeaderMappingService.TipNegLookup() {
            @Override
            public BigDecimal resolveByPaymentMethod(Integer paymentMethodId) {
                return new BigDecimal(9);
            }
        };

        FastchannelHeaderMappingService service = new FastchannelHeaderMappingService(
                (tipo, codExterno) -> null,
                new FastchannelHeaderMappingService.PartnerLookup() {
                    @Override
                    public BigDecimal findCodParcByDocument(String document) {
                        return null;
                    }

                    @Override
                    public BigDecimal findCodVendByParc(BigDecimal codParc) {
                        return null;
                    }
                },
                tipNegLookup,
                config
        );

        Method method = FastchannelHeaderMappingService.class.getDeclaredMethod("resolveTipNeg", OrderDTO.class);
        method.setAccessible(true);
        BigDecimal tipNeg = (BigDecimal) method.invoke(service, order);
        assertEquals(new BigDecimal(9), tipNeg);
    }

    @Test
    public void resolve_mustPreferTopFromDeparaInsteadOfForcing403() throws IOException {
        String src = readMainSource("br/com/bellube/fastchannel/service/FastchannelHeaderMappingService.java");

        assertTrue(src.contains("resolved.codTipOper = resolveByKeys(TIPO_TOP_PEDIDO, keys);"));
        assertTrue(src.contains("resolved.codTipOper = config.getCodTipOper();"));
        assertFalse(src.contains("preferTop403("));
    }

    private String readMainSource(String relativeMainJavaPath) throws IOException {
        Path fromRepoRoot = Paths.get("model", "src", "main", "java").resolve(relativeMainJavaPath);
        if (Files.exists(fromRepoRoot)) {
            return new String(Files.readAllBytes(fromRepoRoot), StandardCharsets.UTF_8);
        }
        Path fromModelRoot = Paths.get("src", "main", "java").resolve(relativeMainJavaPath);
        if (Files.exists(fromModelRoot)) {
            return new String(Files.readAllBytes(fromModelRoot), StandardCharsets.UTF_8);
        }
        throw new IOException("Arquivo fonte nao encontrado: " + relativeMainJavaPath);
    }
}
