<div align="center">
<h1>
  Atenção
</h1><br/>


</div>  


# Recurso Auto DDL Habilitado

O recurso Auto DDL é o responsável pela geração automática de tabelas, que automatia a criação e atualização de tabelas no banco de dados com base no metadados.xml.

É uma ferramenta poderosa que automatiza a criação e atualização de tabelas no banco de dados. Em vez de escrever scripts SQL manuais, você define suas tabelas em um arquivo XML, e o Add-on Studio cuida do resto.

Por esse motivo, a pasta dbscript não traz um exemplo de criação de tabelas. Em vez disso, apresenta um script de criação de função.

O recurso Auto DDL é habilitado no arquivo build.gradle, localizado na raiz do seu Add-on. Caso queira desabilitá-lo, basta alterar na seção addon.

```groovy
addon {
    autoDDL=false
}
```
Dessa forma, os scripts de criação de tabelas devem ser incluídos manualmente na pasta dbscripts, conforme o exemplo abaixo:
```xml
<?xml version="1.0" encoding="ISO-8859-1" ?>
<scripts xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:noNamespaceSchemaLocation="../.gradle/scripts.xsd">
    <sql nomeTabela="TPL_ATENDIMENTO" ordem="1" executar="SE_NAO_EXISTIR" tipoObjeto="TABLE" nomeObjeto="TPL_ATENDIMENTO">
        <oracle>
            CREATE TABLE TPL_ATENDIMENTO(
            CODIGO INTEGER PRIMARY KEY,
            DESCRICAO VARCHAR2(255),
            CATEGORIA_ATENDIMENTO VARCHAR2(255),
            CODUSU INTEGER
            )
        </oracle>
        <mssql>
            CREATE TABLE TPL_ATENDIMENTO(
            CODIGO INT PRIMARY KEY,
            DESCRICAO VARCHAR(255),
            CATEGORIA_ATENDIMENTO VARCHAR(255),
            CODUSU INT
            )
        </mssql>
    </sql>
    <sql nomeTabela="TPL_ATENDIMENTO" ordem="2" executar="SE_NAO_EXISTIR" tipoObjeto="COLUMN" nomeObjeto="DATA_CRIACAO">
        <oracle>
            ALTER TABLE TPL_ATENDIMENTO ADD DATA_CRIACAO TIMESTAMP
        </oracle>
        <mssql>
            ALTER TABLE TPL_ATENDIMENTO ADD DATA_CRIACAO DATETIME
        </mssql>
    </sql>
</scripts>
```

Para mais informações sobre o recurso, acesse: https://developer.sankhya.com.br/docs/02_autoddl
