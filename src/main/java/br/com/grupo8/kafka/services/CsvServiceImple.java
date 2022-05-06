package br.com.grupo8.kafka.services;

import br.com.grupo8.kafka.dao.ProdutosDAO;
import br.com.grupo8.kafka.models.Produto;
import br.com.grupo8.kafka.util.CsvUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.ArrayList;

import static java.time.Instant.now;

@Service
public class CsvServiceImple implements ICsvService{
    @Autowired
    private ProdutosDAO prodDao;

    @Override
    public void salvaProdutosCsv(String aquivo) {
        ArrayList<String[]> dados = CsvUtil.lerCsvProdutos(aquivo);
        for (String[] row : dados) {
            Produto p = new Produto();
            p.setNome(row[0]);
            p.setDescricao(row[1]);
            p.setQuantidade(Integer.parseInt(row[2]));
            p.setDataCadastro(Timestamp.from(now()));
            prodDao.save(p);
        }
    }

}
