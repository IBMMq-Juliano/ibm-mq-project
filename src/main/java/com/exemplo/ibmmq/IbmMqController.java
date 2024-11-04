package com.exemplo.ibmmq;

import com.exemplo.ibmmq.IbmMqService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/mq")
public class IbmMqController {

    @Autowired
    private IbmMqService ibmMqService;

    @PostMapping("/send")
    public ResponseEntity<String> sendMessage(@RequestBody String message) {
        try {
            ibmMqService.sendMessageToQueue(message);
            return ResponseEntity.ok("Mensagem enviada com sucesso!");
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Erro ao enviar mensagem: " + e.getMessage());
        }
    }
}
