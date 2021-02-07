package com.payoneer.cs.store;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import javax.annotation.PostConstruct;

import org.apache.commons.io.FilenameUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.payoneer.cs.error.AppResponseException;
import com.payoneer.cs.job.util.JobResponseErrorCode;

@Service
public class StorageService {

	@Value("${app.fileStore}")
	private String fileStore;

	@PostConstruct
	private void setupStore() throws IOException {
		if (!Paths.get(fileStore).toFile().isDirectory())
			Files.createDirectories(Paths.get(fileStore));
	}

	private String getJobStorePath(String fileName, String fileExtension) {
		return fileStore + "/" + fileName + "." + fileExtension;
	}

	public String saveFile(MultipartFile file, String fileName) {
		if (file.isEmpty())
			throw new AppResponseException(JobResponseErrorCode.RESPONSE_ERROR_001);

		String fileLocation = this.getJobStorePath(fileName, FilenameUtils.getExtension(file.getOriginalFilename()));
		try {
			Files.copy(file.getInputStream(), Paths.get(fileLocation), StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			throw new AppResponseException(JobResponseErrorCode.RESPONSE_ERROR_002);
		}

		return fileLocation;
	}

}