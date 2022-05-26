package broker;

import broker.logic.polling.PollingLogic;
import broker.model.Publication;

import java.security.InvalidParameterException;

public class Broker {

    public static void main(String[] args) {
        if (args.length < 9) {
            throw new InvalidParameterException("Required parameters: {company} {kafkaPublicationsServerHost} {kafkaPublicationsServerPort} {kafkaSpecificPublicationsServerHost} {kafkaSpecificPublicationsServerPort} {kafkaSubscriptionsServerHost} {kafkaSubscriptionsServerPort} {kafkaSpecificSubscriptionsServerHost} {kafkaSpecificSubscriptionsServerPort}");
        }

        String company = args[0];
        String kafkaPublicationsServerHost = args[1];
        int kafkaPublicationsServerPort = Integer.parseInt(args[2]);
        String kafkaSpecificPublicationsServerHost = args[3];
        int kafkaSpecificPublicationsServerPort = Integer.parseInt(args[4]);
        String kafkaSubscriptionsServerHost = args[5];
        int kafkaSubscriptionsServerPort = Integer.parseInt(args[6]);
        String kafkaSpecificSubscriptionsServerHost = args[7];
        int kafkaSpecificSubscriptionsServerPort = Integer.parseInt(args[8]);

        PollingLogic<Publication> publicationsPollingLogic = new PollingLogic<>(company, "Publications", kafkaPublicationsServerHost, kafkaPublicationsServerPort, kafkaSpecificPublicationsServerHost, kafkaSpecificPublicationsServerPort, Publication.getFields(), Publication.class);
        publicationsPollingLogic.start();
    }
}
