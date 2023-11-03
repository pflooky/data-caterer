package com.github.pflooky.datacatererutils.slack;

import com.slack.api.app_backend.slash_commands.response.SlashCommandResponse;
import com.slack.api.bolt.context.builtin.SlashCommandContext;
import com.slack.api.bolt.request.builtin.SlashCommandRequest;
import org.joda.time.DateTime;
import org.joda.time.Days;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TrialUtil {

    private final Map<String, String> userKeys;
    private final Pattern pattern = Pattern.compile("^.+?random ([0-9]{4}-[0-9]{2}-[0-9]{2}) text.*$");
    private final Path userKeysFilePath = Paths.get("/home/ec2-user/user-keys.csv");
    private final int numberOfTrialDays = 31;
    private final KeyEncryption keyEncryption = new KeyEncryption();

    public TrialUtil() throws Exception {
        this.userKeys = initUserKeyMap();
    }

    public SlashCommandResponse generateApiKey(SlashCommandRequest req, SlashCommandContext ctx) {
        SlashCommandResponse slashCommandResponse = new SlashCommandResponse();
        slashCommandResponse.setResponseType("ephemeral");
        try {
            String apiKey;
            if (isExistingUser(ctx)) {
                apiKey = userKeys.get(ctx.getRequestUserId());
            } else {
                apiKey = generateApiKey();
                userKeys.put(ctx.getRequestUserId(), apiKey);
                String newFileEntry = ctx.getRequestUserId() + "," + apiKey + "\n";
                Files.write(userKeysFilePath, newFileEntry.getBytes(), StandardOpenOption.APPEND);
            }

            Optional<DateTime> optionalDateTime = getDate(keyEncryption.decrypt(apiKey));
            int daysLeft = optionalDateTime.map(dateTime -> {
                int daysBetween = Days.daysBetween(DateTime.now(), dateTime.plusDays(numberOfTrialDays)).getDays();
                return Math.max(daysBetween, 0);
            }).orElse(0);
            String message;
            if (daysLeft == 0) {
                message = "Your trial period has expired. Please consider upgrading to the paid plan to help support development https://data.catering/pricing/";
            } else {
                message = "*Your API key is:* `" + apiKey + "`\n*Number of days left for trial:* " + daysLeft + " days";
            }
            slashCommandResponse.setText(message);
        } catch (Exception ex) {
            slashCommandResponse.setText("Failed to generate new API key. Please contact Peter Flook in Slack for support.");
        }
        return slashCommandResponse;
    }

    public String generateApiKey() throws Exception {
        String today = new DateTime().toString("yyyy-MM-dd");
        String baseText = "some% random " + today + " text˚¬å";
        return keyEncryption.encrypt(baseText);
    }

    public boolean isExistingUser(SlashCommandContext ctx) {
        return userKeys.containsKey(ctx.getRequestUserId());
    }

    private Optional<DateTime> getDate(String key) {
        Matcher matcher = pattern.matcher(key);
        if (matcher.matches()) {
            String date = matcher.group(1);
            return Optional.of(DateTime.parse(date));
        } else {
            return Optional.empty();
        }
    }

    private Boolean checkApiKey(String key) throws Exception {
        String apiKeyEnv = key.length() > 0 ? key : System.getenv("API_KEY");
        if (apiKeyEnv == null) {
            return false;
        } else {
            String decryptedApiKey = keyEncryption.decrypt(apiKeyEnv);
            Matcher matcher = pattern.matcher(decryptedApiKey);
            if (matcher.matches()) {
                String date = matcher.group(1);
                DateTime parsedDate = DateTime.parse(date);
                DateTime today = DateTime.now();
                return parsedDate.plusDays(numberOfTrialDays).isBefore(today);
            } else {
                return false;
            }
        }
    }

    private Map<String, String> initUserKeyMap() {
        try {
            List<String> lines = Files.readAllLines(userKeysFilePath);
            return lines.stream().map(line -> {
                String[] spt = line.split(",");
                return Map.entry(spt[0], spt[1]);
            }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        } catch (Exception ex) {
            return Collections.emptyMap();
        }
    }

    static class KeyEncryption {
        private final String key = "getBytesgetStringgetInst";
        private final String initVector = "getInstanceInitt";
        private final IvParameterSpec iv = new IvParameterSpec(initVector.getBytes(StandardCharsets.UTF_8));
        private final SecretKeySpec skeySpec = new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), "AES");
        private final Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");

        KeyEncryption() throws Exception {
        }

        String encrypt(String string) throws Exception {
            cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv);
            byte[] encrypted = cipher.doFinal(string.getBytes());
            return Base64.getEncoder().encodeToString(encrypted);
        }

        String decrypt(String string) throws Exception {
            cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv);
            byte[] decrypted = cipher.doFinal(Base64.getDecoder().decode(string));
            return new String(decrypted);
        }
    }
}
