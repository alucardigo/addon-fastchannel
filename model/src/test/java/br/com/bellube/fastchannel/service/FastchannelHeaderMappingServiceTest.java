package br.com.bellube.fastchannel.service;

import br.com.bellube.fastchannel.dto.OrderCustomerDTO;
import br.com.bellube.fastchannel.dto.OrderDTO;
import br.com.bellube.fastchannel.dto.OrderPaymentDetailsDTO;
import org.junit.Test;

import java.lang.reflect.Method;
import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;

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
}
