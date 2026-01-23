package br.com.bellube.fastchannel.dto;

import java.math.BigDecimal;

/**
 * DTO para dados do cliente em pedidos do Fastchannel.
 *
 * Mapeia para TGFPAR no Sankhya.
 */
public class OrderCustomerDTO {

    // Identificação
    private String customerId;
    private String name;
    private String email;
    private String phone;
    private String cellPhone;

    // Documentos
    private String cpfCnpj;
    private String rg;
    private String stateRegistration; // IE

    // Tipo
    private String personType; // PF ou PJ
    private String companyName; // Razão Social (PJ)

    // Campos Sankhya (transient)
    private transient BigDecimal codParc;
    private transient String tpPessoa;

    public OrderCustomerDTO() {
    }

    // ==================== GETTERS e SETTERS ====================

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getCellPhone() {
        return cellPhone;
    }

    public void setCellPhone(String cellPhone) {
        this.cellPhone = cellPhone;
    }

    public String getCpfCnpj() {
        return cpfCnpj;
    }

    public void setCpfCnpj(String cpfCnpj) {
        this.cpfCnpj = cpfCnpj;
    }

    public String getRg() {
        return rg;
    }

    public void setRg(String rg) {
        this.rg = rg;
    }

    public String getStateRegistration() {
        return stateRegistration;
    }

    public void setStateRegistration(String stateRegistration) {
        this.stateRegistration = stateRegistration;
    }

    public String getPersonType() {
        return personType;
    }

    public void setPersonType(String personType) {
        this.personType = personType;
    }

    public String getCompanyName() {
        return companyName;
    }

    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public BigDecimal getCodParc() {
        return codParc;
    }

    public void setCodParc(BigDecimal codParc) {
        this.codParc = codParc;
    }

    public String getTpPessoa() {
        return tpPessoa;
    }

    public void setTpPessoa(String tpPessoa) {
        this.tpPessoa = tpPessoa;
    }

    /**
     * Verifica se é pessoa jurídica.
     */
    public boolean isPJ() {
        if (personType != null) {
            return "PJ".equalsIgnoreCase(personType);
        }
        // Inferir pelo tamanho do CPF/CNPJ
        if (cpfCnpj != null) {
            String clean = cpfCnpj.replaceAll("[^0-9]", "");
            return clean.length() == 14;
        }
        return false;
    }

    /**
     * Retorna CPF/CNPJ limpo (somente números).
     */
    public String getCleanCpfCnpj() {
        return cpfCnpj != null ? cpfCnpj.replaceAll("[^0-9]", "") : null;
    }

    @Override
    public String toString() {
        return "OrderCustomerDTO{" +
                "name='" + name + '\'' +
                ", cpfCnpj='" + cpfCnpj + '\'' +
                ", email='" + email + '\'' +
                '}';
    }
}
