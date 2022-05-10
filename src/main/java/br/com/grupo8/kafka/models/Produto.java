package br.com.grupo8.kafka.models;

import javax.persistence.*;
import java.sql.Timestamp;

@Entity
@Table(name="produtos")
public class Produto {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false)
    private Integer id;
    @Column(name = "nome", nullable = false)
    private String nome;
    @Column(name = "descricao")
    private String descricao;
    @Column(name = "valor", nullable = false)
    private Double valor;
    @Column(name = "data_cadastro")
    private Timestamp dataCadastro;

    public Produto() {
        super();
    }    

    public Produto(Integer id, String nome, String descricao, Double valor, Timestamp dataCadastro) {
        this.id = id;
        this.nome = nome;
        this.descricao = descricao;
        this.valor = valor;
        this.dataCadastro = dataCadastro;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getNome() {
        return nome;
    }

    public void setNome(String nome) {
        this.nome = nome;
    }

    public String getDescricao() {
        return descricao;
    }

    public void setDescricao(String descricao) {
        this.descricao = descricao;
    }

    public Double getValor() {
        return valor;
    }

    public void setValor(Double valor) {
        this.valor = valor;
    }

    public Timestamp getDataCadastro() {
        return dataCadastro;
    }

    public void setDataCadastro(Timestamp dataCadastro) {
        this.dataCadastro = dataCadastro;
    }

}
