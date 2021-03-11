import type { NextApiRequest, NextApiResponse } from "next";

export default (req: NextApiRequest, res: NextApiResponse) => {
  const { body } = req.body;
  const { event } = body;
  const { text } = event;

  const message = text.replace("<@U018K0G2Y85> ", "");
  console.log(message);
  console.log(req.body.event);

  const { challenge } = req.body;
  res.status(200).send(challenge);
  //   console.log(req.body);
  //   res.status(200).json({ name: "John Doe" });
};
