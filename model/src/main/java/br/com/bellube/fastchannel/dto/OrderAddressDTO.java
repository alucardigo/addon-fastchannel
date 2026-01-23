package br.com.bellube.fastchannel.dto;

import java.math.BigDecimal;

/**
 * DTO para endereço em pedidos do Fastchannel.
 *
 * Mapeia para TGFEND no Sankhya.
 */
public class OrderAddressDTO {

    private String street;
    private String number;
    private String complement;
    private String neighborhood;
    private String city;
    private String state;
    private String zipCode;
    private String country;
    private String reference;

    // Identificação do destinatário
    private String recipientName;
    private String recipientPhone;

    // Campos Sankhya (transient)
    private transient BigDecimal codEnd;
    private transient BigDecimal codCid;
    private transient BigDecimal codBai;

    public OrderAddressDTO() {
        this.country = "BR";
    }

    // ==================== GETTERS e SETTERS ====================

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public String getNumber() {
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
    }

    public String getComplement() {
        return complement;
    }

    public void setComplement(String complement) {
        this.complement = complement;
    }

    public String getNeighborhood() {
        return neighborhood;
    }

    public void setNeighborhood(String neighborhood) {
        this.neighborhood = neighborhood;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getZipCode() {
        return zipCode;
    }

    public void setZipCode(String zipCode) {
        this.zipCode = zipCode;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getReference() {
        return reference;
    }

    public void setReference(String reference) {
        this.reference = reference;
    }

    public String getRecipientName() {
        return recipientName;
    }

    public void setRecipientName(String recipientName) {
        this.recipientName = recipientName;
    }

    public String getRecipientPhone() {
        return recipientPhone;
    }

    public void setRecipientPhone(String recipientPhone) {
        this.recipientPhone = recipientPhone;
    }

    public BigDecimal getCodEnd() {
        return codEnd;
    }

    public void setCodEnd(BigDecimal codEnd) {
        this.codEnd = codEnd;
    }

    public BigDecimal getCodCid() {
        return codCid;
    }

    public void setCodCid(BigDecimal codCid) {
        this.codCid = codCid;
    }

    public BigDecimal getCodBai() {
        return codBai;
    }

    public void setCodBai(BigDecimal codBai) {
        this.codBai = codBai;
    }

    /**
     * Retorna CEP limpo (somente números).
     */
    public String getCleanZipCode() {
        return zipCode != null ? zipCode.replaceAll("[^0-9]", "") : null;
    }

    /**
     * Retorna endereço completo formatado.
     */
    public String getFullAddress() {
        StringBuilder sb = new StringBuilder();
        if (street != null) sb.append(street);
        if (number != null) sb.append(", ").append(number);
        if (complement != null && !complement.isEmpty()) sb.append(" - ").append(complement);
        if (neighborhood != null) sb.append(", ").append(neighborhood);
        if (city != null) sb.append(" - ").append(city);
        if (state != null) sb.append("/").append(state);
        if (zipCode != null) sb.append(" CEP: ").append(zipCode);
        return sb.toString();
    }

    @Override
    public String toString() {
        return "OrderAddressDTO{" +
                "street='" + street + '\'' +
                ", number='" + number + '\'' +
                ", city='" + city + '\'' +
                ", state='" + state + '\'' +
                ", zipCode='" + zipCode + '\'' +
                '}';
    }
}
