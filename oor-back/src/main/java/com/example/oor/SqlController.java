package com.example.oor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/sql")
@CrossOrigin(origins = "http://localhost:4200")

public class SqlController {

    @Autowired
    private SqlService sqlExecutionService;

    @GetMapping("/customer-info/{id}")
    public List<Map<String, Object>> getCustomerInfo(@PathVariable String id) {
        return sqlExecutionService.executeCustomerInfoQuery(id);
    }

    @GetMapping("/last-reactivation-date/{id}")
    public List<Map<String, Object>> getLastReactivationDate(@PathVariable String id) {
        return sqlExecutionService.executeLastReactivationDateQuery(id);
    }

    @GetMapping("/payment-resp/{id}")
    public List<Map<String, Object>> getPaymentResp(@PathVariable String id) {
        return sqlExecutionService.SoldeCXetStatutContrat(id);
    }

    @GetMapping("/solde-CX/{id}")
    public List<Map<String, Object>> getSoldeCX(@PathVariable String id) {
        return sqlExecutionService.SoldeCX(id);
    }

    // New endpoint to handle CSV file upload for customer info
    @PostMapping("/customer-info/upload")
    public List<Map<String, Object>> uploadCustomerInfo(@RequestParam("file") MultipartFile file) throws IOException {
        List<String> ids = readCsvFile(file);
        return sqlExecutionService.executeCustomerInfoQuery(ids.toArray(new String[0]));
    }

    // New endpoint to handle CSV file upload for last reactivation date
    @PostMapping("/last-reactivation-date/upload")
    public List<Map<String, Object>> uploadLastReactivationDate(@RequestParam("file") MultipartFile file) throws IOException {
        List<String> ids = readCsvFile(file);
        return sqlExecutionService.executeLastReactivationDateQuery(ids.toArray(new String[0]));
    }

    // New endpoint to handle CSV file upload for payment responsibility
    @PostMapping("/payment-resp/upload")
    public List<Map<String, Object>> uploadPaymentResp(@RequestParam("file") MultipartFile file) throws IOException {
        List<String> ids = readCsvFile(file);
        return sqlExecutionService.SoldeCXetStatutContrat(ids.toArray(new String[0]));
    }

    // New endpoint to handle CSV file upload for Solde CX
    @PostMapping("/solde-CX/upload")
    public List<Map<String, Object>> uploadSoldeCX(@RequestParam("file") MultipartFile file) throws IOException {
        List<String> ids = readCsvFile(file);
        return sqlExecutionService.SoldeCX(ids.toArray(new String[0]));
    }

    // Utility method to read CSV file and extract custcodes
    private List<String> readCsvFile(MultipartFile file) throws IOException {
        List<String> ids = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(file.getInputStream()))) {
            String line;
            br.readLine(); // Skip header line
            while ((line = br.readLine()) != null) {
                ids.add(line.trim());
            }
        }
        return ids;
    }
}
