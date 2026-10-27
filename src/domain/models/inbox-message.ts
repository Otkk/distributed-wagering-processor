export interface ReceiveInboxProps {
  messageId: string;
  consumerName: string;
  payloadHash: string;
  receivedAt?: Date;
}

export interface InboxMessageState {
  messageId: string;
  consumerName: string;
  payloadHash: string;
  receivedAt: Date;
  processedAt?: Date;
}

export class InboxMessage {
  private constructor(
    public readonly messageId: string,
    public readonly consumerName: string,
    public readonly payloadHash: string,
    public readonly receivedAt: Date,
    private _processedAt?: Date,
  ) {}

  public static receive(props: ReceiveInboxProps): InboxMessage {
    if (!props.messageId || !props.consumerName || !props.payloadHash) {
      throw new Error('messageId, consumerName and payloadHash are required');
    }
    return new InboxMessage(
      props.messageId,
      props.consumerName,
      props.payloadHash,
      props.receivedAt || new Date(),
    );
  }

  public static rehydrate(state: InboxMessageState): InboxMessage {
    return new InboxMessage(
      state.messageId,
      state.consumerName,
      state.payloadHash,
      state.receivedAt,
      state.processedAt,
    );
  }

  public get processedAt(): Date | undefined {
    return this._processedAt;
  }

  public isProcessed(): boolean {
    return this._processedAt !== undefined;
  }

  public markProcessed(at: Date): void {
    this._processedAt = at;
  }
}
