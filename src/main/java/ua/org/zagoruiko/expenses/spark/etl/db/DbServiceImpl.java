package ua.org.zagoruiko.expenses.spark.etl.db;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.SQLException;

@Service
public class DbServiceImpl implements DbService {
    private DataSource dataSource;

    private static final String SQL_REFRESH_TRANSACTION_MATERIALIZED_VIEW = "drop table if exists expenses.multicurrency_transactions;\n" +
            "create table expenses.multicurrency_transactions as\n" +
            "SELECT\n" +
            "    t.id,\n" +
            "    date_trunc('day'::text, t.transaction_date) as transaction_date,\n" +
            "    t.amount,\n" +
            "    t.description,\n" +
            "    t.tags,\n" +
            "    t.category,\n" +
            "    t.currency,\n" +
            "    t.amount / coalesce(r_eur.rate, (SELECT rrr.rate from expenses.rates rrr WHERE date_trunc('day'::text, rrr.date) <= date_trunc('day'::text, t.transaction_date) and rrr.asset='EUR' AND rrr.quote = t.currency order by rrr.date DESC limit 1)) eur_amount,\n" +
            "    t.amount / coalesce(r_usd.rate, (SELECT rrr.rate from expenses.rates rrr WHERE date_trunc('day'::text, rrr.date) <= date_trunc('day'::text, t.transaction_date) and rrr.asset='USD' AND rrr.quote = t.currency order by rrr.date DESC limit 1)) usd_amount,\n" +
            "    t.amount / coalesce(r_czk.rate, (SELECT rrr.rate from expenses.rates rrr WHERE date_trunc('day'::text, rrr.date) <= date_trunc('day'::text, t.transaction_date) and rrr.asset='CZK' AND rrr.quote = t.currency order by rrr.date DESC limit 1)) czk_amount,\n" +
            "    t.amount / coalesce(r_uah.rate, (SELECT rrr.rate from expenses.rates rrr WHERE date_trunc('day'::text, rrr.date) <= date_trunc('day'::text, t.transaction_date) and rrr.asset='UAH' AND rrr.quote = t.currency order by rrr.date DESC limit 1)) uah_amount,\n" +
            "    t.amount / coalesce(r_btc.rate, (SELECT rrr.rate from expenses.rates rrr WHERE date_trunc('day'::text, rrr.date) <= date_trunc('day'::text, t.transaction_date) and rrr.asset='BTC' AND rrr.quote = t.currency order by rrr.date DESC limit 1)) btc_amount\n" +
            "from expenses.transactions t\n" +
            "         left join expenses.rates r_eur\n" +
            "                   ON r_eur.date = date_trunc('day'::text, t.transaction_date)\n" +
            "                       AND t.currency = r_eur.quote and r_eur.asset = 'EUR'\n" +
            "         left join expenses.rates r_usd\n" +
            "                   ON r_usd.date = date_trunc('day'::text, t.transaction_date)\n" +
            "                       AND t.currency = r_usd.quote and r_usd.asset = 'USD'\n" +
            "         left join expenses.rates r_czk\n" +
            "                   ON r_czk.date = date_trunc('day'::text, t.transaction_date)\n" +
            "                       AND t.currency = r_czk.quote and r_czk.asset = 'CZK'\n" +
            "         left join expenses.rates r_uah\n" +
            "                   ON r_uah.date = date_trunc('day'::text, t.transaction_date)\n" +
            "                       AND t.currency = r_uah.quote and r_uah.asset = 'UAH'\n" +
            "         left join expenses.rates r_btc\n" +
            "                   ON r_btc.date = date_trunc('day'::text, t.transaction_date)\n" +
            "                       AND t.currency = r_btc.quote and r_btc.asset = 'BTC';";

    public DbServiceImpl(@Autowired @Qualifier("pgDataSource") DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void refreshTransactionsMaterializedView() throws SQLException {
        dataSource.getConnection().prepareCall(SQL_REFRESH_TRANSACTION_MATERIALIZED_VIEW).execute();
    }
}
