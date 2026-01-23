package br.com.bellube.fastchannel.dto;

import java.math.BigDecimal;
import java.sql.Timestamp;

/**
 * DTO para envio de nota fiscal ao Fastchannel.
 *
 * Mapeia dados da TGFCAB/TGFNOT para o formato Fastchannel.
 */
public class OrderInvoiceDTO {

    private String invoiceNumber;
    private String invoiceSeries;
    private String invoiceKey;
    private Timestamp invoiceDate;
    private BigDecimal totalValue;
    private String invoiceUrl;
    private String xmlUrl;

    // Campos Sankhya (transient - não serializado)
    private transient BigDecimal nuNota;
    private transient BigDecimal codEmp;

    public OrderInvoiceDTO() {
    }

    public OrderInvoiceDTO(String invoiceNumber, String invoiceSeries, String invoiceKey) {
        this.invoiceNumber = invoiceNumber;
        this.invoiceSeries = invoiceSeries;
        this.invoiceKey = invoiceKey;
    }

    // ==================== GETTERS e SETTERS ====================

    public String getInvoiceNumber() {
        return invoiceNumber;
    }

    public void setInvoiceNumber(String invoiceNumber) {
        this.invoiceNumber = invoiceNumber;
    }

    public String getInvoiceSeries() {
        return invoiceSeries;
    }

    public void setInvoiceSeries(String invoiceSeries) {
        this.invoiceSeries = invoiceSeries;
    }

    public String getInvoiceKey() {
        return invoiceKey;
    }

    public void setInvoiceKey(String invoiceKey) {
        this.invoiceKey = invoiceKey;
    }

    public Timestamp getInvoiceDate() {
        return invoiceDate;
    }

    public void setInvoiceDate(Timestamp invoiceDate) {
        this.invoiceDate = invoiceDate;
    }

    public BigDecimal getTotalValue() {
        return totalValue;
    }

    public void setTotalValue(BigDecimal totalValue) {
        this.totalValue = totalValue;
    }

    public String getInvoiceUrl() {
        return invoiceUrl;
    }

    public void setInvoiceUrl(String invoiceUrl) {
        this.invoiceUrl = invoiceUrl;
    }

    public String getXmlUrl() {
        return xmlUrl;
    }

    public void setXmlUrl(String xmlUrl) {
        this.xmlUrl = xmlUrl;
    }

    public BigDecimal getNuNota() {
        return nuNota;
    }

    public void setNuNota(BigDecimal nuNota) {
        this.nuNota = nuNota;
    }

    public BigDecimal getCodEmp() {
        return codEmp;
    }

    public void setCodEmp(BigDecimal codEmp) {
        this.codEmp = codEmp;
    }

    @Override
    public String toString() {
        return "OrderInvoiceDTO{" +
                "invoiceNumber='" + invoiceNumber + '\'' +
                ", invoiceSeries='" + invoiceSeries + '\'' +
                ", invoiceKey='" + invoiceKey + '\'' +
                '}';
    }
}
