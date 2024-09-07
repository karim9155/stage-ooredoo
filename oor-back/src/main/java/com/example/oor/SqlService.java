package com.example.oor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class SqlService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    public List<Map<String, Object>> executeCustomerInfoQuery(String id) {
        String sql = "SELECT DISTINCT\n" +
                "    cu.custcode,\n" +
                "    cc.CCLINE1 AS \"PRENOM_NOM\",\n" +
                "    CONCAT(cc.ccline2, ' ', cc.ccline3) AS \"ADRESSE\",\n" +
                "    cc.CCCITY AS \"DELEGATION\",\n" +
                "    cc.CCSTATE AS \"GOUVERNORAT\",\n" +
                "    cc.CCZIP AS \"CODE POSTAL\",\n" +
                "    cu.CSCURBALANCE AS \"SOLDE CX\",\n" +
                "    cu.PAYMNTRESP AS \"RESP_PAIEMENT\",\n" +
                "    c.co_id,\n" +
                "    c.ch_status AS \"STATUS ACTUEL\",\n" +
                "    r1.des AS \"OFFRE CONTRAT\",\n" +
                "    (SELECT MAX(H2.CH_VALIDFROM)\n" +
                "     FROM contract_history H2\n" +
                "     WHERE H.co_id = H2.co_id\n" +
                "     AND H2.ch_status = 's') AS \"Dernière date de suspension\",\n" +
                "   (SELECT R.RS_DESC\n" +
                "     FROM contract_history H2\n" +
                "     JOIN REASONSTATUS_ALL R ON R.RS_ID = H2.CH_REASON\n" +
                "     WHERE H.co_id = H2.co_id\n" +
                "     AND H2.CH_SEQNO = (\n" +
                "                SELECT MAX(H1.CH_SEQNO)\n" +
                "                FROM contract_history H1\n" +
                "                WHERE H2.co_id = H1.co_id\n" +
                "                AND H1.ch_status = 's')\n" +
                "   ) AS \"MOTIF SUSPENSION\"\n" +
                "FROM\n" +
                "    ccontact_all cc\n" +
                "JOIN customer_all cu ON cc.customer_id = cu.customer_id\n" +
                "JOIN contract_all c ON cu.customer_id = c.customer_id\n" +
                "JOIN contract_history H ON c.co_id = H.co_id\n" +
                "JOIN rateplan r1 ON r1.tmcode = c.tmcode\n" +
                "WHERE\n" +
                "    cc.ccseq = (\n" +
                "       SELECT MAX(ccseq)\n" +
                "       FROM ccontact_all a\n" +
                "       WHERE cc.customer_id = a.customer_id\n" +
                "    )\n" +
                "    AND c.co_id IN (\n" +
                "        SELECT co_id\n" +
                "        FROM contract_all c1\n" +
                "        JOIN OTT_RATEPLAN_DETAILS s ON s.tmcode = c1.tmcode\n" +
                "        WHERE c1.customer_id = c.customer_id\n" +
                "        AND s.prepaid = 'N'\n" +
                "    )\n" +
                "    AND (\n" +
                "        -- ACTIF\n" +
                "        (\n" +
                "            H.CH_SEQNO = (\n" +
                "                SELECT MAX(H1.CH_SEQNO)\n" +
                "                FROM contract_history H1\n" +
                "                WHERE H.co_id = H1.co_id\n" +
                "                AND H1.ch_status = 'a'\n" +
                "                )\n" +
                "            AND c.co_id = (\n" +
                "                SELECT MAX(co_id)\n" +
                "                FROM contract_all c2\n" +
                "                WHERE c2.customer_id = c.customer_id\n" +
                "                AND c2.ch_status = 'a'\n" +
                "                AND c2.tmcode IN (\n" +
                "                    SELECT tmcode\n" +
                "                    FROM OTT_RATEPLAN_DETAILS\n" +
                "                    WHERE prepaid = 'N')\n" +
                "            )\n" +
                "            AND (\n" +
                "                SELECT COUNT(*)\n" +
                "                FROM contract_all c1\n" +
                "                WHERE c1.customer_id = c.customer_id\n" +
                "                AND c1.ch_status = 'a'\n" +
                "                AND c1.tmcode IN (\n" +
                "                    SELECT tmcode\n" +
                "                    FROM OTT_RATEPLAN_DETAILS\n" +
                "                    WHERE prepaid = 'N'\n" +
                "                )\n" +
                "            ) > 0\n" +
                "        )        \n" +
                "        OR\n" +
                "        -- SUSPENDU\n" +
                "        (\n" +
                "            H.CH_SEQNO = (\n" +
                "                SELECT MAX(H1.CH_SEQNO)\n" +
                "                FROM contract_history H1\n" +
                "                WHERE H.co_id = H1.co_id\n" +
                "                AND H1.ch_status = 's'\n" +
                "            )\n" +
                "            AND c.co_id = (\n" +
                "                SELECT MAX(co_id)\n" +
                "                FROM contract_all c2\n" +
                "                WHERE c2.customer_id = c.customer_id\n" +
                "                AND c2.ch_status = 's'\n" +
                "                AND c2.tmcode IN (\n" +
                "                    SELECT tmcode\n" +
                "                    FROM OTT_RATEPLAN_DETAILS\n" +
                "                    WHERE prepaid = 'N')\n" +
                "            )\n" +
                "            AND (\n" +
                "                SELECT COUNT(*)\n" +
                "                FROM contract_all c1\n" +
                "                WHERE c1.customer_id = c.customer_id\n" +
                "                AND c1.ch_status = 'a'\n" +
                "                AND c1.tmcode IN (\n" +
                "                    SELECT tmcode\n" +
                "                    FROM OTT_RATEPLAN_DETAILS\n" +
                "                    WHERE prepaid = 'N'\n" +
                "                )\n" +
                "            ) = 0\n" +
                "            AND (\n" +
                "                SELECT COUNT(*)\n" +
                "                FROM contract_all c1\n" +
                "                WHERE c1.customer_id = c.customer_id\n" +
                "                AND c1.ch_status = 's'\n" +
                "                AND c1.tmcode IN (\n" +
                "                    SELECT tmcode\n" +
                "                    FROM OTT_RATEPLAN_DETAILS\n" +
                "                    WHERE prepaid = 'N'\n" +
                "                )\n" +
                "            ) > 0\n" +
                "        ) OR\n" +
                "        -- DESACTIF\n" +
                "        (\n" +
                "            H.CH_SEQNO = (\n" +
                "                SELECT MAX(H1.CH_SEQNO)\n" +
                "                FROM contract_history H1\n" +
                "                WHERE H.co_id = H1.co_id\n" +
                "                AND H1.ch_status = 'd'\n" +
                "            )\n" +
                "            AND c.co_id = (\n" +
                "                SELECT MAX(co_id)\n" +
                "                FROM contract_all c2\n" +
                "                WHERE c2.customer_id = c.customer_id\n" +
                "                AND c2.ch_status = 'd'\n" +
                "                AND c2.tmcode IN (\n" +
                "                    SELECT tmcode\n" +
                "                    FROM OTT_RATEPLAN_DETAILS\n" +
                "                    WHERE prepaid = 'N')\n" +
                "            )\n" +
                "            AND (\n" +
                "                SELECT COUNT(*)\n" +
                "                FROM contract_all c1\n" +
                "                WHERE c1.customer_id = c.customer_id\n" +
                "                AND c1.ch_status != 'd'\n" +
                "                AND c1.tmcode IN (\n" +
                "                    SELECT tmcode\n" +
                "                    FROM OTT_RATEPLAN_DETAILS\n" +
                "                    WHERE prepaid = 'N'\n" +
                "                )\n" +
                "            ) = 0\n" +
                "        )\n" +
                "    )\n" +
                "    AND cu.custcode = ?;";  // Use a parameter placeholder
        return jdbcTemplate.queryForList(sql, new Object[]{id});
    }


    public List<Map<String, Object>> executeLastReactivationDateQuery(String id) {
        String sql = "select u.custcode,u.CSCURBALANCE, c.co_id, h.ch_status, H.CH_VALIDFROM \n" +
                "  from customer_all u, contract_all c, contract_history h  , contract_history f\n" +
                "where u.customer_id = c.customer_id and f.co_id = c.co_id\n" +
                "   and c.co_id = h.co_id\n" +
                "   and H.CH_SEQNO = (select max(H1.CH_SEQNO) from contract_history h1 where h.co_id = h1.co_id and h1.ch_status = 'a')\n" +
                "   and f.CH_SEQNO = (select max(h2.CH_SEQNO)-1 from contract_history h2 where h.co_id = h2.co_id and h2.ch_status = 's')\n" +
                "   and u.custcode = ?;";  // Use a parameter placeholder
        return jdbcTemplate.queryForList(sql, new Object[]{id});
    }

    public List<Map<String, Object>> SoldeCX(String id) {
        String sql = "select distinct cu.CUSTCODE, cu.CSCURBALANCE \"SOLDE CX\", cu.PAYMNTRESP \"RESP_PAIEMENT\"\n" +
                "  from customer_all cu\n" +
                "where cu.custcode = ?;";  // Use a parameter placeholder
        return jdbcTemplate.queryForList(sql, new Object[]{id});
    }

    public List<Map<String, Object>> SoldeCXetStatutContrat(String id) {
        String sql = "select distinct cu.CUSTCODE, cu.CSCURBALANCE \"SOLDE CX\", c.CO_ID, r.des \"OFFRE\", c.CH_STATUS \"STATUS ACTUEL\",C.CH_STATUS_VALIDFROM \"DATE STATUS\", a.RS_DESC\n" +
                "  from customer_all cu,  contract_all c, OTT_RATEPLAN_DETAILS s, rateplan r, contract_history h, reasonstatus_all a\n" +
                "where cu.customer_id = c.customer_id\n" +
                "  AND s.tmcode = c.tmcode\n" +
                "  AND s.tmcode = r.tmcode\n" +
                "  AND c.co_id = h.co_id\n" +
                "  AND s.prepaid = 'N'\n" +
                "  AND H.CH_SEQNO = (select max(CH_SEQNO) from contract_history h1 where  h1.co_id = h.co_id)\n" +
                "  and H.CH_REASON = A.RS_ID\n" +
                "  and cu.custcode = ?;";  // Use a parameter placeholder
        return jdbcTemplate.queryForList(sql, new Object[]{id});
    }



    //list input///::::::::::::::::::::::::::::::::::::::::::::::::::::::::::


    public List<Map<String, Object>> executeCustomerInfoQuery(String... ids) {
        String sql = "SELECT DISTINCT " +
                "    cu.custcode, " +
                "    cc.CCLINE1 AS `PRENOM_NOM`, " +
                "    CONCAT(cc.ccline2, ' ', cc.ccline3) AS `ADRESSE`, " +
                "    cc.CCCITY AS `DELEGATION`, " +
                "    cc.CCSTATE AS `GOUVERNORAT`, " +
                "    cc.CCZIP AS `CODE POSTAL`, " +
                "    cu.CSCURBALANCE AS `SOLDE CX`, " +
                "    cu.PAYMNTRESP AS `RESP_PAIEMENT`, " +
                "    c.co_id, " +
                "    c.ch_status AS `STATUS ACTUEL`, " +
                "    r1.des AS `OFFRE CONTRAT`, " +
                "    (SELECT MAX(H2.CH_VALIDFROM) " +
                "     FROM contract_history H2 " +
                "     WHERE H.co_id = H2.co_id " +
                "     AND H2.ch_status = 's') AS `Dernière date de suspension`, " +
                "   (SELECT R.RS_DESC " +
                "     FROM contract_history H2 " +
                "     JOIN REASONSTATUS_ALL R ON R.RS_ID = H2.CH_REASON " +
                "     WHERE H.co_id = H2.co_id " +
                "     AND H2.CH_SEQNO = ( " +
                "                SELECT MAX(H1.CH_SEQNO) " +
                "                FROM contract_history H1 " +
                "                WHERE H2.co_id = H1.co_id " +
                "                AND H1.ch_status = 's') " +
                "   ) AS `MOTIF SUSPENSION` " +
                "FROM ccontact_all cc " +
                "JOIN customer_all cu ON cc.customer_id = cu.customer_id " +
                "JOIN contract_all c ON cu.customer_id = c.customer_id " +
                "JOIN contract_history H ON c.co_id = H.co_id " +
                "JOIN rateplan r1 ON r1.tmcode = c.tmcode " +
                "WHERE cc.ccseq = ( " +
                "       SELECT MAX(ccseq) " +
                "       FROM ccontact_all a " +
                "       WHERE cc.customer_id = a.customer_id " +
                "    ) " +
                "    AND c.co_id IN ( " +
                "        SELECT co_id " +
                "        FROM contract_all c1 " +
                "        JOIN OTT_RATEPLAN_DETAILS s ON s.tmcode = c1.tmcode " +
                "        WHERE c1.customer_id = c.customer_id " +
                "        AND s.prepaid = 'N' " +
                "    ) " +
                "    AND ( " +
                "        ( " +
                "            H.CH_SEQNO = ( " +
                "                SELECT MAX(H1.CH_SEQNO) " +
                "                FROM contract_history H1 " +
                "                WHERE H.co_id = H1.co_id " +
                "                AND H1.ch_status = 'a' " +
                "            ) " +
                "            AND c.co_id = ( " +
                "                SELECT MAX(co_id) " +
                "                FROM contract_all c2 " +
                "                WHERE c2.customer_id = c.customer_id " +
                "                AND c2.ch_status = 'a' " +
                "                AND c2.tmcode IN ( " +
                "                    SELECT tmcode " +
                "                    FROM OTT_RATEPLAN_DETAILS " +
                "                    WHERE prepaid = 'N') " +
                "            ) " +
                "            AND ( " +
                "                SELECT COUNT(*) " +
                "                FROM contract_all c1 " +
                "                WHERE c1.customer_id = c.customer_id " +
                "                AND c1.ch_status = 'a' " +
                "                AND c1.tmcode IN ( " +
                "                    SELECT tmcode " +
                "                    FROM OTT_RATEPLAN_DETAILS " +
                "                    WHERE prepaid = 'N' " +
                "                ) " +
                "            ) > 0 " +
                "        ) " +
                "        OR " +
                "        ( " +
                "            H.CH_SEQNO = ( " +
                "                SELECT MAX(H1.CH_SEQNO) " +
                "                FROM contract_history H1 " +
                "                WHERE H.co_id = H1.co_id " +
                "                AND H1.ch_status = 's' " +
                "            ) " +
                "            AND c.co_id = ( " +
                "                SELECT MAX(co_id) " +
                "                FROM contract_all c2 " +
                "                WHERE c2.customer_id = c.customer_id " +
                "                AND c2.ch_status = 's' " +
                "                AND c2.tmcode IN ( " +
                "                    SELECT tmcode " +
                "                    FROM OTT_RATEPLAN_DETAILS " +
                "                    WHERE prepaid = 'N') " +
                "            ) " +
                "            AND ( " +
                "                SELECT COUNT(*) " +
                "                FROM contract_all c1 " +
                "                WHERE c1.customer_id = c.customer_id " +
                "                AND c1.ch_status = 'a' " +
                "                AND c1.tmcode IN ( " +
                "                    SELECT tmcode " +
                "                    FROM OTT_RATEPLAN_DETAILS " +
                "                    WHERE prepaid = 'N' " +
                "                ) " +
                "            ) = 0 " +
                "            AND ( " +
                "                SELECT COUNT(*) " +
                "                FROM contract_all c1 " +
                "                WHERE c1.customer_id = c.customer_id " +
                "                AND c1.ch_status = 's' " +
                "                AND c1.tmcode IN ( " +
                "                    SELECT tmcode " +
                "                    FROM OTT_RATEPLAN_DETAILS " +
                "                    WHERE prepaid = 'N' " +
                "                ) " +
                "            ) > 0 " +
                "        ) OR " +
                "        ( " +
                "            H.CH_SEQNO = ( " +
                "                SELECT MAX(H1.CH_SEQNO) " +
                "                FROM contract_history H1 " +
                "                WHERE H.co_id = H1.co_id " +
                "                AND H1.ch_status = 'd' " +
                "            ) " +
                "            AND c.co_id = ( " +
                "                SELECT MAX(co_id) " +
                "                FROM contract_all c2 " +
                "                WHERE c2.customer_id = c.customer_id " +
                "                AND c2.ch_status = 'd' " +
                "                AND c2.tmcode IN ( " +
                "                    SELECT tmcode " +
                "                    FROM OTT_RATEPLAN_DETAILS " +
                "                    WHERE prepaid = 'N') " +
                "            ) " +
                "            AND ( " +
                "                SELECT COUNT(*) " +
                "                FROM contract_all c1 " +
                "                WHERE c1.customer_id = c.customer_id " +
                "                AND c1.ch_status != 'd' " +
                "                AND c1.tmcode IN ( " +
                "                    SELECT tmcode " +
                "                    FROM OTT_RATEPLAN_DETAILS " +
                "                    WHERE prepaid = 'N' " +
                "                ) " +
                "            ) = 0 " +
                "        ) " +
                "    ) " +
                "    AND cu.custcode IN (" + buildPlaceholders(ids.length) + ")";

        return jdbcTemplate.queryForList(sql, (Object[]) ids);
    }


    public List<Map<String, Object>> executeLastReactivationDateQuery(String... ids) {
        String sql = "SELECT u.custcode, u.CSCURBALANCE, c.co_id, h.ch_status, H.CH_VALIDFROM " +
                "FROM customer_all u " +
                "JOIN contract_all c ON u.customer_id = c.customer_id " +
                "JOIN contract_history h ON c.co_id = h.co_id " +
                "JOIN contract_history f ON f.co_id = c.co_id " +
                "WHERE H.CH_SEQNO = (SELECT MAX(H1.CH_SEQNO) FROM contract_history h1 WHERE h.co_id = h1.co_id AND h1.ch_status = 'a') " +
                "AND f.CH_SEQNO = (SELECT MAX(h2.CH_SEQNO) - 1 FROM contract_history h2 WHERE h.co_id = h2.co_id AND h2.ch_status = 's') " +
                "AND u.custcode IN (" + buildPlaceholders(ids.length) + ")";

        return jdbcTemplate.queryForList(sql, (Object[]) ids);
    }


    public List<Map<String, Object>> SoldeCX(String... ids) {
        String sql = "SELECT DISTINCT cu.CUSTCODE, cu.CSCURBALANCE AS `SOLDE CX`, cu.PAYMNTRESP AS `RESP_PAIEMENT` " +
                "FROM customer_all cu " +
                "WHERE cu.custcode IN (" + buildPlaceholders(ids.length) + ")";
        return jdbcTemplate.queryForList(sql, (Object[]) ids);
    }
    public List<Map<String, Object>> SoldeCXetStatutContrat(String... ids) {
        String sql = "SELECT DISTINCT cu.CUSTCODE, cu.CSCURBALANCE AS `SOLDE CX`, c.CO_ID, r.des AS `OFFRE`, " +
                "c.CH_STATUS AS `STATUS ACTUEL`, C.CH_STATUS_VALIDFROM AS `DATE STATUS`, a.RS_DESC " +
                "FROM customer_all cu " +
                "JOIN contract_all c ON cu.customer_id = c.customer_id " +
                "JOIN OTT_RATEPLAN_DETAILS s ON s.tmcode = c.tmcode " +
                "JOIN rateplan r ON s.tmcode = r.tmcode " +
                "JOIN contract_history h ON c.co_id = h.co_id " +
                "JOIN reasonstatus_all a ON H.CH_REASON = A.RS_ID " +
                "WHERE s.prepaid = 'N' " +
                "AND H.CH_SEQNO = (SELECT MAX(CH_SEQNO) FROM contract_history h1 WHERE h1.co_id = h.co_id) " +
                "AND cu.custcode IN (" + buildPlaceholders(ids.length) + ")";

        return jdbcTemplate.queryForList(sql, (Object[]) ids);
    }

    // Utility method to build SQL placeholders
    private String buildPlaceholders(int count) {
        return String.join(",", java.util.Collections.nCopies(count, "?"));
    }
}
