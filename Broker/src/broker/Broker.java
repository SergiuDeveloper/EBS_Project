package broker;

import broker.logic.PublicationsPollingLogic;

import java.security.InvalidParameterException;

public class Broker {

    public static void main(String[] args) {
        if (args.length < 5) {
            throw new InvalidParameterException("Required parameters: {company} {kafkaPublicationsServerHost} {kafkaPublicationsServerPort} {kafkaSpecificPublicationsServerHost} {kafkaSpecificPublicationsServerPort}");
        }

        String company = args[0];
        String kafkaPublicationsServerHost = args[1];
        int kafkaPublicationsServerPort = Integer.parseInt(args[2]);
        String kafkaSpecificPublicationsServerHost = args[3];
        int kafkaSpecificPublicationsServerPort = Integer.parseInt(args[4]);

        PublicationsPollingLogic publicationsPollingLogic = new PublicationsPollingLogic(company, kafkaPublicationsServerHost, kafkaPublicationsServerPort, kafkaSpecificPublicationsServerHost, kafkaSpecificPublicationsServerPort);
        publicationsPollingLogic.start();
    }
}
