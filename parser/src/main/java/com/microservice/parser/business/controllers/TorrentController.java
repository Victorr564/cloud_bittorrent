package com.microservice.parser.business.controllers;

import com.microservice.parser.presentation.controllers.KafkaController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

@Controller
public class TorrentController {
    @Autowired
    KafkaController kafkaController;
    @PostMapping("/add_torrent")
    @ResponseBody
    public void getTorrent(@RequestParam("torrent") MultipartFile file)
    {
        try {
            kafkaController.sendTorrentMetadata(file.getBytes(),"torrent.parsed","key");
        }
        catch(IOException e)
        {
            e.printStackTrace();
        }
    }

}
