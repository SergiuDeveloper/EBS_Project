package broker;

import broker.logic.polling.PollingLogic;
import broker.model.Publication;

import java.security.InvalidParameterException;

public class Broker {

    public static void main(String[] args) {
        if (args.length < 3) {
            throw new InvalidParameterException("Required parameters: {company} {kafkaServerHost} {kafkaServerPort}");
        }

        String company = args[0];
        String kafkaServerHost = args[1];
        int kafkaServerPort = Integer.parseInt(args[2]);

        PollingLogic<Publication> publicationsPollingLogic = new PollingLogic<>(company, "Publications", kafkaServerHost, kafkaServerPort, Publication.getFields(), Publication.class);
        publicationsPollingLogic.start();
    }
}
